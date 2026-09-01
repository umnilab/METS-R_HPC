# SAE J2735 ASN.1/UPER payloads

The V2X bridge can carry either the J2735-aligned JSON semantics or
an actual ASN.1 value encoded with Unaligned Packed Encoding Rules (UPER). SAE
J2735 schemas are licensed, revisioned artifacts, so this repository does not
bundle or recreate one. Supply the exact ASN.1 modules used by every endpoint.

## Enable strict UPER

Install the optional compiler in the same Python environment as the bridge
client:

```bash
python -m pip install -r requirements-j2735.txt
```

Then configure the client with your schema paths and revision:

```json
{
  "veins_j2735_codec": "uper",
  "veins_j2735_asn1_files": [
    "/secure/sae-j2735/J2735.asn"
  ],
  "veins_j2735_asn1_type": "BasicSafetyMessage",
  "veins_j2735_schema_revision": "J2735ASN_202409",
  "veins_j2735_allow_fallback": false,
  "veins_j2735_require_geodetic": true,
  "veins_j2735_require_vehicle_size": true,
  "veins_j2735_decode_received": true
}
```

`uper` is fail-fast by default. Missing schema files, a missing `asn1tools`
installation, a missing configured type, or a compile error stops client
initialization with an actionable error. `auto` permits a labelled
`SAE J2735-aligned` JSON fallback; `aligned` explicitly selects the legacy
representation. A strict experiment should use `uper` and leave
`veins_j2735_allow_fallback` false.

`asn1tools` supports UPER but not every ASN.1 information-object or
parameterization feature. The supplied modules must be a compiler-compatible,
complete set. If a purchased schema uses unsupported constructs, use a
tool-generated codec adapter or pass a custom codec object to `VeinsClient`;
do not replace the schema with the test fixture in `tests/fixtures`.
Custom codecs implement `encode_message(record)` and may implement
`annotate_decoded(record)` and `describe()` for receive decoding and handshake
metadata.

## Semantic mapping

The built-in mapper targets `BasicSafetyMessage` and constructs `coreData`.
It accepts an exact caller-provided `asn1_value`/`j2735_asn1_value` instead, or
a Python `value_builder` when constructing the codec directly. These override
the built-in mapping and support vendor/compiler-specific value shapes.

The default mapper uses:

| J2735 field | Accepted bridge input |
| --- | --- |
| `msgCnt` | `message_count`, `msgCnt`, or tick |
| temporary `id` | `temporary_id`, `vehicle_id`, `vid`, or `sender_id` |
| `secMark` | `secMark`, `sec_mark`, `timestamp_ms`, or `tx_time_s` |
| `lat`/`long` | latitude/longitude degrees or explicit E-7 integer fields |
| `elev` | explicit `elevation` (metres), `elevation_dm`, or encoded `elev` |
| speed/heading/acceleration | SI semantic fields or explicit encoded fields |
| `size` | explicit vehicle width/length in centimetres or metres |

CARLA/METS-R `x`, `y`, and `z` are local map coordinates. They are never
treated as J2735 latitude, longitude, or geodetic elevation. When strict
geodetic validation is disabled, missing position/elevation fields use the
standard unavailable sentinels. Missing vehicle dimensions use J2735's zero
unavailable values, not a guessed vehicle model. Enable the two `require_*`
options to reject incomplete records instead. Strict mapping also rejects
blank, non-finite, non-numeric, unavailable-sentinel geodetic values and zero
or invalid dimensions. Fields named `latitude_e7`/`longitude_e7` are validated
as encoded integers and are never rescaled as degrees.

Automatic `MessageFrame` construction is available for schemas whose Python
mapping is a `messageId` plus a `BasicSafetyMessage` choice. Because generated
open-type mappings vary among ASN.1 toolchains and schema revisions,
`BasicSafetyMessage` or an exact caller-provided value/builder is safer unless
the frame mapping has been verified against the deployment compiler.

## JSON bridge envelope

JSON carries the binary application PDU as opaque base64 data:

```json
{
  "message_standard": "SAE J2735",
  "payload_encoding": "uper",
  "payload_bytes": 42,
  "wire_payload": {
    "encoding": "uper",
    "data_b64": "...",
    "byte_length": 42,
    "bit_length": 336,
    "asn1_type": "BasicSafetyMessage",
    "schema_revision": "J2735ASN_202409",
    "schema_sha256": "..."
  }
}
```

`payload_bytes` is derived from the encoded octets and replaces any synthetic
size supplied by the tutorial. Semantic fields remain beside the opaque PDU
for routing, attacks, metrics, and observability. The network backend must
transport `wire_payload` unchanged. On reception, the client verifies the
schema fingerprint and lengths, decodes UPER, and adds `j2735_decode_status`
and `j2735_decoded` to delivered records.

If retained semantic diagnostics contain ASN.1 OCTET STRING or BIT STRING
Python values, the client converts those nested bytes to explicit base64 JSON
objects before writing the JSON-lines bridge protocol.

UPER encoding alone does not add IEEE 1609.2 signing/encryption, certificate
handling, or application validation rules from SAE J2945/x. Those are separate
pipeline layers and must be configured when interoperability requires them.
