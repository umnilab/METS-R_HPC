"""Optional SAE J2735 ASN.1/UPER payload support for V2X bridge clients.

The SAE ASN.1 modules are revisioned, licensed artifacts and are deliberately
not copied into this repository. Strict UPER mode therefore requires both a
user-supplied schema and the optional :mod:`asn1tools` dependency. The legacy
semantic JSON representation remains an explicitly labelled compatibility
mode.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import importlib
import math
import os
from collections.abc import Mapping, Sequence
from pathlib import Path

from clients.KafkaDataProcessor import build_bsm_core_data


UPER_ENCODING = "uper"
WIRE_PAYLOAD_FIELD = "wire_payload"
DEFAULT_ASN1_TYPE = "BasicSafetyMessage"
DEFAULT_MESSAGE_FRAME_ID = 20


class J2735CodecError(RuntimeError):
    """Base error for J2735 payload preparation or decoding."""


class J2735CodecUnavailable(J2735CodecError):
    """Raised when strict UPER mode cannot be initialized."""


def _config_get(config, name, default=None):
    if config is None:
        return default
    if isinstance(config, Mapping):
        return config.get(name, default)
    return getattr(config, name, default)


def _first_config(config, names, default=None):
    for name in names:
        value = _config_get(config, name)
        if value is not None:
            return value
    return default


def _as_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off", ""}:
            return False
    return bool(value)


def _split_schema_files(value):
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike)):
        text = os.fspath(value).strip()
        if not text:
            return []
        path = Path(text).expanduser()
        if path.exists():
            values = [path]
        else:
            separator = os.pathsep if os.pathsep in text else ","
            values = [
                Path(item.strip()).expanduser()
                for item in text.split(separator)
                if item.strip()
            ]
    elif isinstance(value, Sequence):
        values = [Path(os.fspath(item)).expanduser() for item in value]
    else:
        raise J2735CodecUnavailable(
            "J2735 ASN.1 files must be a path, a path-separated string, or a list."
        )

    files = []
    for path in values:
        if path.is_dir():
            matches = sorted(
                candidate
                for pattern in ("*.asn", "*.asn1")
                for candidate in path.glob(pattern)
                if candidate.is_file()
            )
            if not matches:
                raise J2735CodecUnavailable(
                    f"J2735 ASN.1 directory contains no .asn or .asn1 files: {path}"
                )
            files.extend(matches)
        else:
            files.append(path)
    return files


def _schema_digest(paths):
    file_digests = sorted(hashlib.sha256(path.read_bytes()).digest() for path in paths)
    digest = hashlib.sha256()
    for file_digest in file_digests:
        digest.update(file_digest)
    return digest.hexdigest()


def _message_name(record):
    name = record.get("message_name")
    if name:
        return str(name)
    frame = record.get("messageFrame")
    if isinstance(frame, Mapping):
        frame_id = frame.get("messageId")
        if frame_id in (
            "basicSafetyMessage",
            "BasicSafetyMessage",
            DEFAULT_MESSAGE_FRAME_ID,
        ):
            return "BasicSafetyMessage"
    if "BasicSafetyMessage" in record or "coreData" in record:
        return "BasicSafetyMessage"
    return ""


def is_bsm_record(record):
    """Return whether *record* represents a Basic Safety Message."""
    return (
        isinstance(record, Mapping)
        and _message_name(record).lower() == "basicsafetymessage"
    )


def _wheel_brakes_bit_string(value):
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], (bytes, bytearray))
    ):
        return (bytes(value[0]), value[1])
    if isinstance(value, (bytes, bytearray)):
        return (bytes(value[:1]) or b"\x00", 5)

    names = {
        "unavailable": 0,
        "leftfront": 1,
        "leftrear": 2,
        "rightfront": 3,
        "rightrear": 4,
    }
    selected = set()
    if isinstance(value, str):
        selected.update(
            part.strip().replace("_", "").lower() for part in value.split(",")
        )
    elif isinstance(value, Sequence):
        selected.update(
            str(part).strip().replace("_", "").lower() for part in value
        )
    elif isinstance(value, int):
        selected.update(name for name, bit in names.items() if value & (1 << bit))

    encoded = 0
    for name in selected:
        bit = names.get(name)
        if bit is not None:
            encoded |= 1 << (7 - bit)
    return (bytes([encoded]), 5)


def _finite_number(value):
    if (
        value is None
        or isinstance(value, bool)
        or (isinstance(value, str) and not value.strip())
    ):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _geodetic_source(record, core_source, axis):
    if axis == "latitude":
        candidates = (
            (core_source, "lat", True),
            (record, "latitude_e7", True),
            (record, "lat_e7", True),
            (record, "latitude", False),
            (record, "lat", False),
        )
    else:
        candidates = (
            (core_source, "long", True),
            (record, "longitude_e7", True),
            (record, "lon_e7", True),
            (record, "longitude", False),
            (record, "lon", False),
            (record, "long", False),
        )
    for source, key, encoded in candidates:
        if key in source:
            return key, source.get(key), encoded
    return None, None, False


def _normalized_geodetic(record, core_source, axis, required):
    field, raw_value, encoded = _geodetic_source(record, core_source, axis)
    unavailable = 900000001 if axis == "latitude" else 1800000001
    encoded_minimum = -900000000 if axis == "latitude" else -1799999999
    encoded_maximum = unavailable
    degree_limit = 90.0 if axis == "latitude" else 180.0
    number = _finite_number(raw_value)

    invalid_reason = None
    if field is None:
        invalid_reason = "is missing"
    elif number is None:
        invalid_reason = f"field {field!r} must be a finite number"
    elif encoded and not number.is_integer():
        invalid_reason = f"encoded field {field!r} must be an integer"
    elif encoded and not encoded_minimum <= number <= encoded_maximum:
        invalid_reason = f"encoded field {field!r} is outside the J2735 range"
    elif not encoded and not -degree_limit <= number <= degree_limit:
        invalid_reason = f"degree field {field!r} is outside [-{degree_limit}, {degree_limit}]"

    if invalid_reason is not None:
        if required:
            raise J2735CodecError(
                f"Strict geodetic BSM mapping: {axis} {invalid_reason}. "
                "Cartesian x/y are not valid J2735 coordinates."
            )
        return unavailable

    encoded_value = int(number) if encoded else int(round(number * 10_000_000))
    if not encoded:
        encoded_value = max(encoded_minimum, min(unavailable - 1, encoded_value))
    if required and encoded_value == unavailable:
        raise J2735CodecError(
            f"Strict geodetic BSM mapping: {axis} uses the unavailable sentinel."
        )
    return encoded_value


def _vehicle_dimension_source(record, core_source, dimension):
    size = core_source.get("size")
    if isinstance(size, Mapping) and dimension in size:
        return f"coreData.size.{dimension}", size.get(dimension), "cm"
    record_size = record.get("size")
    if isinstance(record_size, Mapping) and dimension in record_size:
        return f"size.{dimension}", record_size.get(dimension), "cm"
    for key in (f"vehicle_{dimension}_cm", f"{dimension}_cm"):
        if key in record:
            return key, record.get(key), "cm"
    for key in (f"vehicle_{dimension}_m", f"{dimension}_m"):
        if key in record:
            return key, record.get(key), "m"
    if dimension in record:
        return dimension, record.get(dimension), "auto"
    return None, None, "cm"


def _normalized_vehicle_dimension(record, core_source, dimension, required):
    field, raw_value, unit = _vehicle_dimension_source(record, core_source, dimension)
    number = _finite_number(raw_value)
    maximum = 1023 if dimension == "width" else 4095
    invalid_reason = None
    if field is None:
        invalid_reason = "is missing"
    elif number is None:
        invalid_reason = f"field {field!r} must be a finite number"
    else:
        if unit == "m" or (unit == "auto" and 0 < number < 50):
            number *= 100.0
        encoded = int(round(number))
        if not 1 <= encoded <= maximum:
            invalid_reason = (
                f"field {field!r} is outside the J2735 range 1..{maximum} cm"
            )

    if invalid_reason is not None:
        if required:
            raise J2735CodecError(
                f"Strict BSM vehicle-size mapping: {dimension} {invalid_reason}."
            )
        return 0
    return encoded


def _core_data_for_asn1(
    record,
    require_geodetic=False,
    require_vehicle_size=False,
):
    source = dict(record)
    core_source = next(
        (
            source[key]
            for key in ("coreData", "BSMcoreData", "core_data")
            if isinstance(source.get(key), Mapping)
        ),
        {},
    )
    has_geodetic_elevation = (
        core_source.get("elev") is not None
        or any(
            source.get(key) is not None
            for key in ("elev", "elevation", "elevation_dm")
        )
    )
    if not has_geodetic_elevation:
        # METS-R/CARLA z is a local map height, not geodetic J2735 elevation.
        source.pop("z", None)
    if not any(key in source for key in ("secMark", "sec_mark", "timestamp_ms")):
        tx_time_s = source.get("tx_time_s")
        if tx_time_s is not None:
            source["timestamp_ms"] = round(float(tx_time_s) * 1000.0) % 60000

    latitude = _normalized_geodetic(
        source, core_source, "latitude", require_geodetic
    )
    longitude = _normalized_geodetic(
        source, core_source, "longitude", require_geodetic
    )
    width = _normalized_vehicle_dimension(
        source, core_source, "width", require_vehicle_size
    )
    length = _normalized_vehicle_dimension(
        source, core_source, "length", require_vehicle_size
    )
    core = build_bsm_core_data(source)
    core["lat"] = latitude
    core["long"] = longitude
    # Always override the Kafka display fallback. On the wire, zero means that
    # the dimension is unavailable; guessed Tesla dimensions are never used.
    core["size"] = {"width": width, "length": length}
    temporary_id = core.get("id")
    if isinstance(temporary_id, str):
        try:
            core["id"] = bytes.fromhex(temporary_id)
        except ValueError as exc:
            raise J2735CodecError(
                f"BSM temporary id is not an 8-digit hexadecimal value: {temporary_id!r}"
            ) from exc
    brakes = dict(core.get("brakes") or {})
    brakes["wheelBrakes"] = _wheel_brakes_bit_string(brakes.get("wheelBrakes"))
    core["brakes"] = brakes
    accel_source = core_source.get("accelSet")
    accel_source = accel_source if isinstance(accel_source, Mapping) else {}
    if accel_source.get("yaw") is None and source.get("yaw_rate_deg_s") is None:
        core["accelSet"]["yaw"] = 32767
    return core


def build_basic_safety_message_value(
    record,
    require_geodetic=False,
    require_vehicle_size=False,
):
    """Map bridge semantics into an ASN.1 ``BasicSafetyMessage`` value.

    Cartesian ``x``/``y`` fields are intentionally not converted to latitude
    and longitude. Callers must provide geodetic fields; otherwise the valid
    J2735 unavailable sentinels are encoded.
    """
    explicit = record.get("j2735_asn1_value", record.get("asn1_value"))
    if explicit is not None:
        if not isinstance(explicit, Mapping):
            raise J2735CodecError(
                "j2735_asn1_value/asn1_value must be a mapping for BasicSafetyMessage."
            )
        return dict(explicit)
    return {
        "coreData": _core_data_for_asn1(
            record,
            require_geodetic=require_geodetic,
            require_vehicle_size=require_vehicle_size,
        )
    }


def _json_safe(value):
    if isinstance(value, (bytes, bytearray)):
        return {"bytes_b64": base64.b64encode(bytes(value)).decode("ascii")}
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], (bytes, bytearray))
        and isinstance(value[1], int)
    ):
        return {
            "bits_b64": base64.b64encode(bytes(value[0])).decode("ascii"),
            "bit_length": value[1],
        }
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], str):
        return {"choice": value[0], "value": _json_safe(value[1])}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _optional_envelope_length(envelope, field):
    if field not in envelope or envelope.get(field) is None:
        return None
    raw_value = envelope.get(field)
    try:
        if isinstance(raw_value, bool):
            raise ValueError
        if isinstance(raw_value, int):
            value = raw_value
        elif isinstance(raw_value, str) and raw_value.strip().lstrip("+").isdigit():
            value = int(raw_value.strip())
        else:
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise J2735CodecError(
            f"Received J2735 payload {field} must be a non-negative integer."
        ) from exc
    if value < 0:
        raise J2735CodecError(
            f"Received J2735 payload {field} must be a non-negative integer."
        )
    return value


class AlignedJ2735Codec:
    """Compatibility codec retaining the pre-existing semantic JSON payload."""

    mode = "aligned"

    def __init__(self, fallback_reason=None, requested_mode="aligned"):
        self.fallback_reason = fallback_reason
        self.requested_mode = requested_mode

    def describe(self):
        result = {
            "requested": self.requested_mode,
            "active": "sae-j2735-aligned-json",
            "status": (
                "compatibility_fallback" if self.fallback_reason else "configured"
            ),
        }
        if self.fallback_reason:
            result["reason"] = self.fallback_reason
        return result

    def encode_message(self, record):
        message = dict(record)
        message.pop("j2735_asn1_value", None)
        message.pop("asn1_value", None)
        if is_bsm_record(message):
            # Compatibility JSON is not an ASN.1 J2735 message. Override a
            # caller's optimistic label so aligned/fallback mode can never be
            # mistaken for standards-encoded UPER on the wire.
            message["message_standard"] = "SAE J2735-aligned"
            if self.fallback_reason:
                message["j2735_codec"] = self.describe()
        return _json_safe(message)

    def decode_message(self, record):
        return None


class Asn1ToolsJ2735UperCodec:
    """Encode/decode J2735 values with user-supplied ASN.1 modules."""

    mode = "uper"

    def __init__(
        self,
        schema_files,
        type_name=DEFAULT_ASN1_TYPE,
        schema_revision=None,
        message_frame_id=DEFAULT_MESSAGE_FRAME_ID,
        specification=None,
        value_builder=None,
        require_geodetic=False,
        require_vehicle_size=False,
    ):
        self.schema_files = _split_schema_files(schema_files)
        self.type_name = str(type_name or DEFAULT_ASN1_TYPE)
        self.schema_revision = (
            None if schema_revision is None else str(schema_revision)
        )
        self.message_frame_id = int(message_frame_id)
        self.value_builder = value_builder
        self.require_geodetic = bool(require_geodetic)
        self.require_vehicle_size = bool(require_vehicle_size)

        if not self.schema_files:
            raise J2735CodecUnavailable(
                "Strict J2735 UPER mode requires user-supplied ASN.1 files. Set "
                "veins_j2735_asn1_files to the licensed, revision-matched J2735 schema."
            )
        missing = [str(path) for path in self.schema_files if not path.is_file()]
        if missing:
            raise J2735CodecUnavailable(
                "J2735 ASN.1 file(s) do not exist: " + ", ".join(missing)
            )
        self.schema_sha256 = _schema_digest(self.schema_files)

        if specification is None:
            try:
                asn1tools = importlib.import_module("asn1tools")
            except ModuleNotFoundError as exc:
                raise J2735CodecUnavailable(
                    "J2735 UPER requires the optional 'asn1tools' package. Install "
                    "requirements-j2735.txt in the Python environment running the client."
                ) from exc
            try:
                specification = asn1tools.compile_files(
                    [str(path) for path in self.schema_files],
                    codec="uper",
                    numeric_enums=False,
                )
            except Exception as exc:
                raise J2735CodecUnavailable(
                    "Could not compile the supplied J2735 ASN.1 modules for UPER. "
                    "Verify all imported modules, the peer's schema revision, and "
                    f"asn1tools compatibility. Compiler error: {exc}"
                ) from exc
        self.specification = specification

        available_types = getattr(specification, "types", None)
        if isinstance(available_types, Mapping) and self.type_name not in available_types:
            preview = ", ".join(sorted(available_types)[:12])
            raise J2735CodecUnavailable(
                f"ASN.1 type {self.type_name!r} was not found in the supplied modules. "
                f"Available unique types include: {preview or '(none)'}."
            )

    def describe(self):
        result = {
            "requested": "uper",
            "active": "sae-j2735-asn1-uper",
            "status": "configured",
            "asn1_type": self.type_name,
            "schema_sha256": self.schema_sha256,
        }
        if self.schema_revision:
            result["schema_revision"] = self.schema_revision
        return result

    @staticmethod
    def _core_source(record):
        for key in ("coreData", "BSMcoreData", "core_data"):
            value = record.get(key)
            if isinstance(value, Mapping):
                return value
        return {}

    def _mapped_bsm(self, record):
        core_source = self._core_source(record)
        has_id = any(
            record.get(key) is not None
            for key in ("temporary_id", "vehicle_id", "vid", "sender_id")
        ) or core_source.get("id") is not None
        if not has_id:
            raise J2735CodecError(
                "BSM UPER mapping requires temporary_id, vehicle_id, vid, sender_id, "
                "or coreData.id."
            )

        return build_basic_safety_message_value(
            record,
            require_geodetic=self.require_geodetic,
            require_vehicle_size=self.require_vehicle_size,
        )

    def _asn1_value(self, record):
        explicit = record.get("j2735_asn1_value", record.get("asn1_value"))
        if self.value_builder is not None:
            try:
                return self.value_builder(record)
            except Exception as exc:
                raise J2735CodecError(
                    f"The configured J2735 value builder rejected the BSM: {exc}"
                ) from exc
        if explicit is not None:
            return explicit
        if self.type_name == "BasicSafetyMessage":
            return self._mapped_bsm(record)
        if self.type_name == "MessageFrame":
            return {
                "messageId": self.message_frame_id,
                "value": ("BasicSafetyMessage", self._mapped_bsm(record)),
            }
        raise J2735CodecError(
            "Automatic semantic mapping is only defined for BasicSafetyMessage "
            f"and MessageFrame, not {self.type_name!r}; provide asn1_value or a value_builder."
        )

    def encode_message(self, record):
        message = dict(record)
        message.pop("j2735_asn1_value", None)
        message.pop("asn1_value", None)
        envelope = message.get(WIRE_PAYLOAD_FIELD)
        has_uper_envelope = isinstance(envelope, Mapping) and str(
            envelope.get("encoding", "")
        ).lower() in {"uper", "asn1-uper", "j2735-uper"}
        has_explicit_value = any(
            key in record and record.get(key) is not None
            for key in ("j2735_asn1_value", "asn1_value")
        )
        if not (
            is_bsm_record(record)
            or has_explicit_value
            or has_uper_envelope
        ):
            return _json_safe(message)
        if has_uper_envelope:
            self.decode_message(message)
            encoded = base64.b64decode(str(envelope["data_b64"]), validate=True)
            message["message_standard"] = "SAE J2735"
            message["payload_encoding"] = UPER_ENCODING
            message["payload_bytes"] = len(encoded)
            message["j2735_codec"] = self.describe()
            return _json_safe(message)
        value = self._asn1_value(record)
        try:
            encoded = bytes(
                self.specification.encode(
                    self.type_name,
                    value,
                    check_types=True,
                    check_constraints=True,
                )
            )
        except Exception as exc:
            message_id = record.get(
                "message_id", record.get("message_count", "unknown")
            )
            raise J2735CodecError(
                f"Could not encode J2735 {self.type_name} message {message_id!r}: {exc}"
            ) from exc

        envelope = {
            "encoding": UPER_ENCODING,
            "data_b64": base64.b64encode(encoded).decode("ascii"),
            "byte_length": len(encoded),
            "bit_length": len(encoded) * 8,
            "asn1_type": self.type_name,
            "schema_sha256": self.schema_sha256,
        }
        if self.schema_revision:
            envelope["schema_revision"] = self.schema_revision
        message.update(
            {
                "message_standard": "SAE J2735",
                "payload_encoding": UPER_ENCODING,
                "payload_bytes": len(encoded),
                WIRE_PAYLOAD_FIELD: envelope,
                "j2735_codec": self.describe(),
            }
        )
        return _json_safe(message)

    def decode_message(self, record):
        envelope = (
            record.get(WIRE_PAYLOAD_FIELD) if isinstance(record, Mapping) else record
        )
        if not isinstance(envelope, Mapping):
            return None
        encoding = str(envelope.get("encoding", "")).lower()
        if encoding not in {"uper", "asn1-uper", "j2735-uper"}:
            return None
        encoded_type = envelope.get("asn1_type", self.type_name)
        if encoded_type != self.type_name:
            raise J2735CodecError(
                f"Received ASN.1 type {encoded_type!r}; expected {self.type_name!r}."
            )
        fingerprint = envelope.get("schema_sha256")
        if not isinstance(fingerprint, str) or not fingerprint.strip():
            raise J2735CodecError(
                "Received J2735 payload is missing the required schema_sha256 fingerprint."
            )
        if fingerprint.strip().lower() != self.schema_sha256:
            raise J2735CodecError(
                "Received J2735 payload schema fingerprint does not match the configured schema."
            )
        try:
            encoded = base64.b64decode(str(envelope["data_b64"]), validate=True)
        except (KeyError, ValueError, binascii.Error) as exc:
            raise J2735CodecError(
                "Received J2735 wire_payload contains invalid base64 data."
            ) from exc
        expected_bytes = _optional_envelope_length(envelope, "byte_length")
        if expected_bytes is not None and expected_bytes != len(encoded):
            raise J2735CodecError(
                f"Received J2735 payload is {len(encoded)} bytes; expected {expected_bytes}."
            )
        expected_bits = _optional_envelope_length(envelope, "bit_length")
        if expected_bits is not None and expected_bits != len(encoded) * 8:
            raise J2735CodecError(
                "Received J2735 payload bit_length does not match its octet payload."
            )
        try:
            return self.specification.decode(self.type_name, encoded)
        except Exception as exc:
            raise J2735CodecError(
                f"Could not decode J2735 {self.type_name} UPER payload: {exc}"
            ) from exc

    def annotate_decoded(self, record):
        message = dict(record)
        if WIRE_PAYLOAD_FIELD not in message:
            return message
        try:
            decoded = self.decode_message(message)
        except J2735CodecError as exc:
            message["j2735_decode_status"] = "error"
            message["j2735_decode_error"] = str(exc)
        else:
            message["j2735_decode_status"] = "ok"
            message["j2735_decoded"] = _json_safe(decoded)
        return message


def create_j2735_codec(
    config=None,
    mode=None,
    schema_files=None,
    type_name=None,
    schema_revision=None,
    allow_fallback=None,
    specification=None,
    value_builder=None,
):
    """Create a strict UPER or compatibility codec.

    ``uper`` is strict unless fallback is explicitly enabled. ``auto`` tries
    UPER when a schema is configured and otherwise uses a labelled semantic
    fallback. ``aligned`` always retains the legacy JSON representation.
    """
    requested = str(
        mode
        if mode is not None
        else _first_config(
            config,
            ("veins_j2735_codec", "veins_payload_codec", "j2735_codec"),
            "aligned",
        )
    ).strip().lower()
    if requested in {"aligned", "semantic", "json", "legacy"}:
        return AlignedJ2735Codec(requested_mode=requested)
    if requested not in {"auto", "uper", "asn1-uper", "j2735-uper"}:
        raise ValueError(
            f"Unsupported J2735 codec mode {requested!r}; use aligned, auto, or uper."
        )

    configured_files = (
        schema_files
        if schema_files is not None
        else _first_config(
            config,
            (
                "veins_j2735_asn1_files",
                "veins_j2735_schema_files",
                "j2735_asn1_files",
            ),
            os.environ.get("J2735_ASN1_FILES"),
        )
    )
    configured_type = type_name or _first_config(
        config,
        ("veins_j2735_asn1_type", "j2735_asn1_type"),
        DEFAULT_ASN1_TYPE,
    )
    configured_revision = (
        schema_revision
        if schema_revision is not None
        else _first_config(
            config,
            ("veins_j2735_schema_revision", "j2735_schema_revision"),
        )
    )
    fallback_allowed = requested == "auto"
    if allow_fallback is not None:
        fallback_allowed = _as_bool(allow_fallback)
    else:
        configured_fallback = _first_config(
            config, ("veins_j2735_allow_fallback",)
        )
        if configured_fallback is not None:
            fallback_allowed = _as_bool(configured_fallback)

    try:
        return Asn1ToolsJ2735UperCodec(
            schema_files=configured_files,
            type_name=configured_type,
            schema_revision=configured_revision,
            message_frame_id=_first_config(
                config,
                ("veins_j2735_message_frame_id", "j2735_message_frame_id"),
                DEFAULT_MESSAGE_FRAME_ID,
            ),
            specification=specification,
            value_builder=value_builder,
            require_geodetic=_as_bool(
                _first_config(config, ("veins_j2735_require_geodetic",), False)
            ),
            require_vehicle_size=_as_bool(
                _first_config(config, ("veins_j2735_require_vehicle_size",), False)
            ),
        )
    except J2735CodecUnavailable as exc:
        if not fallback_allowed:
            raise
        return AlignedJ2735Codec(
            fallback_reason=str(exc), requested_mode=requested
        )


def encode_j2735_messages(codec, records):
    """Encode BSM records while leaving unrelated load messages intact."""
    return [
        codec.encode_message(record) if isinstance(record, Mapping) else record
        for record in records or []
    ]


def annotate_decoded_j2735_messages(codec, records):
    """Decode received UPER envelopes when supported by the active codec."""
    annotate = getattr(codec, "annotate_decoded", None)
    if annotate is None:
        return list(records or [])
    return [
        annotate(record) if isinstance(record, Mapping) else record
        for record in records or []
    ]
