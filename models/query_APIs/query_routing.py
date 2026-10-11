"""Query APIs for routing."""

import gzip
import hashlib
import json
import os
import posixpath
import tempfile
import threading

import networkx as nx

from utils.configuration import _read_property_values


class QueryRoutingMixin:
    """Query APIs for routing."""

    _ROUTING_TOPOLOGY_SCHEMA_VERSION = 2

    _ROUTING_TOPOLOGY_CACHE_LOCK = threading.Lock()

    _ROUTING_TOPOLOGY_MEMORY_CACHE = {}

    _NETWORK_SHA256_CACHE = {}

    def query_route(self, orig_x, orig_y, dest_x, dest_y, transform_coords = False):
        """Query the shortest path between two coordinate pairs.

        Each query pair returns a connector-aware route record::

            {
              'roadIds': [<physical_road_id>, ...],
              'connectorIds': [<source>_<target>, ...],
              'segmentIds': [<road>, <connector>, ...],
              'roadDistance': <meters>,
              'connectorDistance': <meters>,
              'distance': <meters>,
              'roadTravelTime': <seconds>,
              'connectorTravelTime': <seconds>,
              'travelTime': <seconds>,
              'travelTimeP90': <seconds>,
              'travelTimeConfidence': <0-to-1>,
              'minimumSegmentTravelTimeConfidence': <0-to-1>,
              'liveEvidenceSegmentCount': <int>,
              'priorOnlySegmentCount': <int>
            }
        The server returns ``'error'`` if no path was found.

        Parameters
        ----------
        orig_x, orig_y : float | list[float]
            Origin coordinate(s) in the SIM/internal frame, or projected/local
            SUMO coordinates when ``transform_coords=True``.
        dest_x, dest_y : float | list[float]
            Destination coordinate(s) (matched pairwise with origin).
        transform_coords : bool | list[bool]
            ``True`` if the input coordinates are projected/local SUMO
            coordinates and should be transformed into the SIM/internal frame
            before routing.
        """
        msg = {"messageType": "routesBwCoords", "data": []}
        if not isinstance(orig_x, list):
            orig_x = [orig_x]
            orig_y = [orig_y]
            dest_x = [dest_x]
            dest_y = [dest_y]

        if not isinstance(transform_coords, list):
            transform_coords = [transform_coords] * len(orig_x)

        assert len(orig_x) == len(orig_y) == len(dest_x) == len(dest_y), "Length of orig_x, orig_y, dest_x, and dest_y must be the same"

        for orig_x, orig_y, dest_x, dest_y, transform_coord in zip(orig_x, orig_y, dest_x, dest_y, transform_coords):
            msg["data"].append({
                "originX": orig_x,
                "originY": orig_y,
                "destinationX": dest_x,
                "destinationY": dest_y,
                "transformCoordinates": transform_coord,
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["messageType"] == "routesBwCoords", res["messageType"]
        return res

    def query_k_routes(self, orig_x, orig_y, dest_x, dest_y, k, transform_coords = False):
        """Query the *k* shortest paths between two coordinate pairs.

        Each query pair returns physical-only ``roadIdLists`` plus parallel
        connector-aware arrays::

            {
              'roadIdLists': [[<physical_road_id>, ...], ...],
              'connectorIdLists': [[<connector_id>, ...], ...],
              'segmentIdLists': [[<road_or_connector>, ...], ...],
              'roadDistances': [<meters>, ...],
              'connectorDistances': [<meters>, ...],
              'distances': [<meters>, ...],
              'roadTravelTimes': [<seconds>, ...],
              'connectorTravelTimes': [<seconds>, ...],
              'travelTimes': [<seconds>, ...],
              'travelTimeP90s': [<seconds>, ...],
              'travelTimeConfidences': [<0-to-1>, ...],
              'minimumSegmentTravelTimeConfidences': [<0-to-1>, ...],
              'liveEvidenceSegmentCounts': [<int>, ...],
              'priorOnlySegmentCounts': [<int>, ...]
            }

        or ``'error'`` if no path was found.

        Parameters
        ----------
        orig_x, orig_y : float | list[float]
            Origin coordinate(s).
        dest_x, dest_y : float | list[float]
            Destination coordinate(s).
        k : int | list[int]
            Number of alternative routes to return per query.
        transform_coords : bool | list[bool]
            ``True`` to interpret coordinates as projected/local SUMO
            coordinates and transform them into the SIM/internal frame.
        """
        msg = {"messageType": "multiRoutesBwCoords", "data": []}
        if not isinstance(orig_x, list):
            orig_x = [orig_x]
            orig_y = [orig_y]
            dest_x = [dest_x]
            dest_y = [dest_y]
            k = [k]
        if not isinstance(transform_coords, list):
            transform_coords = [transform_coords] * len(orig_x)
        if not isinstance(k, list):
            k = [k] * len(orig_x)

        assert len(orig_x) == len(orig_y) == len(dest_x) == len(dest_y), "Length of orig_x, orig_y, dest_x, and dest_y must be the same"

        for orig_x, orig_y, dest_x, dest_y, transform_coord, k in zip(orig_x, orig_y, dest_x, dest_y, transform_coords, k):
            msg["data"].append({
                "originX": orig_x,
                "originY": orig_y,
                "destinationX": dest_x,
                "destinationY": dest_y,
                "transformCoordinates": transform_coord,
                "routeCount": k,
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "multiRoutesBwCoords", res["messageType"]
        return res

    def query_route_between_roads(self, orig_road, dest_road):
        """Query the shortest path between two roads identified by their SUMO IDs.

        Each query pair returns a connector-aware route record::

            {
              'roadIds': [<physical_road_id>, ...],
              'connectorIds': [<source>_<target>, ...],
              'segmentIds': [<road_or_connector>, ...],
              'distance': <meters>,
              'travelTime': <seconds>,
              'travelTimeP90': <seconds>,
              'travelTimeConfidence': <0-to-1>,
              'minimumSegmentTravelTimeConfidence': <0-to-1>,
              'liveEvidenceSegmentCount': <int>,
              'priorOnlySegmentCount': <int>
            }

        Separate physical-road and connector distance/travel-time fields are
        also returned. The server returns ``'error'`` if no path was found.

        Parameters
        ----------
        orig_road : str | list[str]
            SUMO road ID(s) of the origin road.
        dest_road : str | list[str]
            SUMO road ID(s) of the destination road (matched pairwise).
        """
        msg = {"messageType": "routesBwRoads", "data": []}
        if not isinstance(orig_road, list):
            orig_road = [orig_road]

        if not isinstance(dest_road, list):
            dest_road = [dest_road] * len(orig_road)
        assert len(orig_road) == len(dest_road), "Length of orig_road and dest_road must be the same"

        for orig_road, dest_road in zip(orig_road, dest_road):
            msg["data"].append({
                "originRoadId": orig_road,
                "destinationRoadId": dest_road,
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)

        assert res["messageType"] == "routesBwRoads", res["messageType"]
        return res

    def query_k_routes_between_roads(self, orig_road, dest_road, k):
        """Query the *k* shortest paths between two roads.

        Each query pair returns physical-only ``roadIdLists`` plus connector
        IDs, interleaved paths, and per-route distance/travel-time arrays::

            {'roadIdLists': [[<physical_road_id>, ...], ...],
             'connectorIdLists': [[<connector_id>, ...], ...],
             'segmentIdLists': [[<road_or_connector>, ...], ...],
             'distances': [<meters>, ...],
             'travelTimes': [<seconds>, ...],
             'travelTimeP90s': [<seconds>, ...],
             'travelTimeConfidences': [<0-to-1>, ...],
             'minimumSegmentTravelTimeConfidences': [<0-to-1>, ...],
             'liveEvidenceSegmentCounts': [<int>, ...],
             'priorOnlySegmentCounts': [<int>, ...]}

        or ``'error'`` if no path was found.

        Parameters
        ----------
        orig_road : str | list[str]
            SUMO road ID(s) of the origin road.
        dest_road : str | list[str]
            SUMO road ID(s) of the destination road.
        k : int | list[int]
            Number of alternative routes to return per query.
        """
        msg = {"messageType": "multiRoutesBwRoads", "data": []}
        if not isinstance(orig_road, list):
            orig_road = [orig_road]
            dest_road = [dest_road]
            k = [k]
        if not isinstance(k, list):
            k = [k] * len(orig_road)

        assert len(orig_road) == len(dest_road), "Length of orig_road and dest_road must be the same"

        for orig_road, dest_road, k in zip(orig_road, dest_road, k):
            msg["data"].append({
                "originRoadId": orig_road,
                "destinationRoadId": dest_road,
                "routeCount": k,
            })

        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "multiRoutesBwRoads", res["messageType"]
        return res

    @staticmethod
    def _routing_truthy(value):
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "y")
        return bool(value)

    @staticmethod
    def _routing_float(record, *names, default=0.0):
        value = next(
            (record[name] for name in names if record.get(name) is not None),
            default,
        ) if isinstance(record, dict) else default
        try:
            number = float(value)
        except (TypeError, ValueError):
            return float(default)
        if number != number or number in (float("inf"), float("-inf")):
            return float(default)
        return number

    @classmethod
    def _routing_downstream_from(cls, record, default=None):
        downstream = record.get("downstreamIds", default)
        if downstream is None:
            return None
        if isinstance(downstream, str):
            return [downstream]
        return [str(road_id) for road_id in downstream if road_id is not None]

    @classmethod
    def _routing_node_attrs_from(cls, record, previous=None):
        previous = previous or {}
        distance = cls._routing_float(
            record, "length", default=previous.get("distance", 0.0)
        )
        speed_limit = cls._routing_float(
            record, "speedLimit", default=previous.get("speed_limit", 0.0)
        )
        travel_time = cls._routing_float(
            record, "travelTime", default=previous.get("travel_time", 0.0)
        )
        if travel_time <= 0.0 and distance > 0.0 and speed_limit > 0.0:
            travel_time = distance / speed_limit

        energy_consumed = cls._routing_float(
            record, "energyConsumed", default=previous.get("energy_consumed", 0.0)
        )
        avg_energy = cls._routing_float(
            record, "averageEnergyConsumption",
            default=previous.get("avg_energy_consumption", 0.0),
        )
        weight = cls._routing_float(
            record, "routingWeight", default=travel_time
        )
        if weight <= 0.0:
            weight = travel_time

        attrs = {
            "distance": distance,
            "travel_time": travel_time,
            "energy_consumption": avg_energy,
            "avg_energy_consumption": avg_energy,
            "energy_consumed": energy_consumed,
            "total_energy": energy_consumed,
            "length": distance,
            "weight": weight,
            "speed_limit": speed_limit,
        }
        estimator_float_fields = (
            ("travelTimeP90", "travel_time_p90", travel_time),
            ("travelTimeConfidence", "travel_time_confidence", 0.0),
            (
                "travelTimeEffectiveSampleCount",
                "travel_time_effective_sample_count",
                0.0,
            ),
            (
                "travelTimeSampleAgeSeconds",
                "travel_time_sample_age_seconds",
                -1.0,
            ),
            (
                "travelTimeStoppedFraction",
                "travel_time_stopped_fraction",
                0.0,
            ),
            (
                "travelTimeLiveLowerBound",
                "travel_time_live_lower_bound",
                0.0,
            ),
            (
                "travelTimeLiveMeanSpeed",
                "travel_time_live_mean_speed",
                0.0,
            ),
        )
        for response_key, attr_key, default in estimator_float_fields:
            if response_key in record:
                attrs[attr_key] = cls._routing_float(
                    record, response_key, default=default
                )
            elif attr_key in previous:
                attrs[attr_key] = previous[attr_key]
        if "travelTimeLiveVehicleCount" in record:
            try:
                attrs["travel_time_live_vehicle_count"] = int(
                    record["travelTimeLiveVehicleCount"]
                )
            except (TypeError, ValueError):
                attrs["travel_time_live_vehicle_count"] = 0
        elif "travel_time_live_vehicle_count" in previous:
            attrs["travel_time_live_vehicle_count"] = previous[
                "travel_time_live_vehicle_count"
            ]
        if record.get("travelTimeEstimateSource") is not None:
            attrs["travel_time_estimate_source"] = str(
                record["travelTimeEstimateSource"]
            )
        elif "travel_time_estimate_source" in previous:
            attrs["travel_time_estimate_source"] = previous[
                "travel_time_estimate_source"
            ]
        for name in (
                "segmentId", "segmentType", "visualizationIndex",
                "vehicleCount", "speed", "flow", "parkingCapacity",
                "parkedVehicleCount", "status"):
            if record.get(name) is not None:
                attrs[name] = record[name]
        if "visualizationIndex" in attrs:
            attrs["road_index"] = attrs["visualizationIndex"]
        return attrs

    @staticmethod
    def _routing_edge_attrs_from(node_attrs):
        keys = (
            "distance",
            "travel_time",
            "energy_consumption",
            "avg_energy_consumption",
            "energy_consumed",
            "total_energy",
            "length",
            "weight",
            "speed_limit",
            "travel_time_p90",
            "travel_time_confidence",
            "travel_time_effective_sample_count",
            "travel_time_sample_age_seconds",
            "travel_time_live_vehicle_count",
            "travel_time_stopped_fraction",
            "travel_time_live_lower_bound",
            "travel_time_live_mean_speed",
            "travel_time_estimate_source",
            "r_type",
        )
        return {key: node_attrs[key] for key in keys if key in node_attrs}

    @classmethod
    def _apply_routing_graph_metadata(cls, graph, response):
        graph.graph["edge_cost_road"] = "source"
        graph.graph["snapshot_required"] = cls._routing_truthy(response.get("snapshotRequired", False))
        metadata_fields = (
            ("tick", "tick"),
            ("topologyVersion", "topology_version"),
            ("metricVersion", "metric_version"),
            ("metricVersion", "weight_version"),
            ("version", "metric_version"),
            ("version", "weight_version"),
            ("weightVersion", "weight_version"),
        )
        for response_key, graph_key in metadata_fields:
            if response_key in response:
                graph.graph[graph_key] = response[response_key]

    def query_routing_graph_updates(self):
        """Query road routing-metric deltas since the last full road snapshot.

        The endpoint returns ``data`` records only for roads whose
        routing metrics changed. If topology changed, the response sets
        ``snapshotRequired`` and callers should rebuild with
        :meth:`query_routing_graph`.
        """
        msg = {"messageType": "routingGraphUpdates"}
        res = self.send_receive_msg(msg, ignore_heartbeats=True)
        assert res["messageType"] == "routingGraphUpdates", res["messageType"]
        return res

    def _routing_batch_settings(self, batch_size=None):
        if batch_size is None:
            batch_size = self._client_config_value("routing_graph_batch_size", 5000)
        batch_size = max(1, int(batch_size))
        minimum = max(
            1,
            int(self._client_config_value("routing_graph_min_batch_size", 500)),
        )
        minimum = min(minimum, batch_size)
        target_bytes = int(self._client_config_value(
            "routing_graph_response_bytes",
            max(1024, int(self.websocket_max_size * 0.75)),
        ))
        target_bytes = max(1024, min(target_bytes, self.websocket_max_size - 1))
        return batch_size, minimum, target_bytes

    def _recover_routing_connection(self):
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass
        self.ws = None
        self._connect(**self._connection_settings)

    @staticmethod
    def _routing_response_size_error(error):
        current = error
        for _ in range(4):
            if current is None:
                break
            detail = (
                type(current).__name__ + " " + str(current)
            ).lower().replace("-", " ")
            if any(marker in detail for marker in (
                    "payloadtoobig",
                    "payload too big",
                    "message too big",
                    "frame exceeds limit",
                    "max_size",
                    "close code 1009",
                    "received 1009")):
                return True
            current = current.__cause__ or current.__context__
        return False

    def _query_road_records_adaptive(self, road_ids, batch_size=None):
        requested_batch, minimum_batch, target_bytes = self._routing_batch_settings(
            batch_size
        )
        records = []
        metadata = {}
        offset = 0
        current_batch = requested_batch
        while offset < len(road_ids):
            count = min(current_batch, len(road_ids) - offset)
            batch = road_ids[offset:offset + count]
            try:
                response = self.send_receive_msg(
                    {"messageType": "road", "data": batch},
                    ignore_heartbeats=True,
                    max_attempts=1,
                    return_errors=True,
                )
            except (RuntimeError, TimeoutError) as exc:
                if count <= minimum_batch or (
                    isinstance(exc, RuntimeError) and not self._routing_response_size_error(exc)
                ):
                    raise
                self._recover_routing_connection()
                current_batch = max(minimum_batch, count // 2)
                continue
            if response.get("messageType") != "road":
                raise RuntimeError(
                    f"Expected road, received {response.get('messageType')}"
                )
            if str(response.get("status", "ok")).lower() == "error":
                raise RuntimeError(f"METS-R SIM rejected road query batch: {response}")

            records.extend(response.get("data", []))
            metadata = response
            offset += count
            response_bytes = max(1, int(self._last_response_bytes))
            if response_bytes > target_bytes and count > minimum_batch:
                scaled = int(count * target_bytes * 0.85 / response_bytes)
                current_batch = max(minimum_batch, min(count, scaled))
        self._last_routing_batch_size = current_batch
        return records, metadata

    def query_routing_topology(
            self,
            offset=0,
            limit=None,
            include_center=False,
            compact=True,
            legacy_fallback=True):
        """Query one page of static road topology.

        The native endpoint returns the compact ``roadId``,
        ``downstreamRoadId``, and ``length`` schema. If capabilities explicitly
        disable it, the same shape is synthesized from the native ``road`` query.
        """
        offset = max(0, int(offset))
        if limit is not None:
            limit = max(0, int(limit))
        if self._supports_feature("routingTopology"):
            msg = {
                "messageType": "routingTopology",
                "offset": offset,
                "includeCenter": bool(include_center),
                "compact": bool(compact),
            }
            if limit is not None:
                msg["limit"] = limit
            response = self.send_receive_msg(
                msg,
                ignore_heartbeats=True,
                max_attempts=1,
                return_errors=True,
            )
            if not self._is_unsupported_response(response):
                if response.get("messageType") != "routingTopology":
                    raise RuntimeError(
                        "Expected routingTopology, received "
                        f"{response.get('messageType')}"
                    )
                if str(response.get("status", "ok")).lower() != "ok":
                    raise RuntimeError(
                        f"METS-R SIM rejected routingTopology query: {response}"
                    )
                return response
            self._feature_support["routingTopology"] = False

        if include_center:
            raise RuntimeError(
                "include_center requires a METS-R SIM with routingTopology support"
            )
        if not legacy_fallback:
            raise RuntimeError("Connected METS-R SIM does not support routingTopology")
        index_response = self.query_road()
        if str(index_response.get("status", "ok")).lower() == "error":
            raise RuntimeError(f"METS-R SIM rejected road query: {index_response}")
        road_ids = index_response.get("roadIds") or []
        total = len(road_ids)
        end = total if limit is None else min(total, offset + limit)
        selected = road_ids[min(offset, total):end]
        road_records, metadata = self._query_road_records_adaptive(
            selected,
            batch_size=limit,
        ) if selected else ([], index_response)
        static_records = self._static_topology_records(road_records)
        if compact:
            data = [
                [
                    record["segmentId"],
                    record["downstreamIds"],
                    record["length"],
                ]
                for record in static_records
            ]
        else:
            data = static_records
        return {
            "messageType": "routingTopology",
            "status": "ok",
            "data": data,
            "offset": min(offset, total),
            "count": len(data),
            "total": total,
            "hasMore": end < total,
            "topologyVersion": metadata.get(
                "topologyVersion",
                index_response.get("topologyVersion"),
            ),
            "tick": metadata.get("tick", self.current_tick),
            "compact": bool(compact),
            "schema": ["roadId", "downstreamRoadId", "length"] if compact else None,
            "legacyFallback": True,
        }

    def _routing_topology_cache_key(self, topology_version, include_center=False):
        """Identify static topology by its network contents and schema version."""
        if topology_version is None:
            return None
        configured = self._client_config_value("network_file", None)
        if not configured and self.sim_folder:
            properties = _read_property_values(
                os.path.join(self.sim_folder, "data", "Data.properties")
            )
            configured = properties.get("NETWORK_FILE")
        if not configured:
            return None
        configured = os.fspath(configured)
        candidates = []

        # Thin runs map container input paths back to immutable host data.
        if self.sim_folder:
            manifest_path = os.path.join(
                self.sim_folder, "data", ".metsr_hpc_inputs.json"
            )
            try:
                with open(manifest_path, "r", encoding="utf-8") as manifest_file:
                    manifest = json.load(manifest_file)
                if not isinstance(manifest, dict) or int(manifest.get("schema_version")) != 1:
                    raise ValueError("Unsupported input manifest")
                host_source = os.path.expanduser(os.fspath(manifest.get("host_data_source")))
                container_target = os.fspath(manifest.get("container_data_target")).replace("\\", "/")
                if not os.path.isabs(host_source) or not posixpath.isabs(container_target):
                    raise ValueError("Input manifest paths must be absolute")
                host_source = os.path.abspath(host_source)
                container_target = posixpath.normpath(container_target)
                configured_posix = posixpath.normpath(configured.replace("\\", "/"))
                relative_path = None
                try:
                    if posixpath.commonpath((configured_posix, container_target)) == container_target:
                        relative_path = posixpath.relpath(configured_posix, container_target)
                except ValueError:
                    pass
                if relative_path is None:
                    if configured_posix == "data":
                        relative_path = "."
                    elif configured_posix.startswith("data/"):
                        relative_path = configured_posix[len("data/"):]
                    else:
                        raise ValueError("Network is outside the input manifest")
                candidate = os.path.abspath(os.path.join(host_source, *relative_path.split("/")))
                if os.path.commonpath((candidate, host_source)) == host_source:
                    candidates.append(candidate)
            except (OSError, TypeError, ValueError):
                pass

        if os.path.isabs(configured):
            candidates.append(configured)
        else:
            if self.sim_folder:
                candidates.append(os.path.join(self.sim_folder, configured))
            candidates.append(os.path.abspath(configured))
            if self.config_json:
                candidates.append(os.path.join(os.path.dirname(self.config_json), configured))
        network_file = next(
            (os.path.abspath(path) for path in candidates if os.path.isfile(path)),
            None,
        )
        if network_file is None:
            return None

        # Reuse a digest only while the file's size and modification time match.
        try:
            stat = os.stat(network_file)
        except OSError:
            return None
        stat_key = (
            network_file,
            int(stat.st_size),
            int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1e9))),
        )
        with self._ROUTING_TOPOLOGY_CACHE_LOCK:
            network_sha256 = self._NETWORK_SHA256_CACHE.get(stat_key)
            if network_sha256 is None:
                digest = hashlib.sha256()
                try:
                    with open(network_file, "rb") as network_stream:
                        for chunk in iter(lambda: network_stream.read(1024 * 1024), b""):
                            digest.update(chunk)
                except OSError:
                    return None
                network_sha256 = digest.hexdigest()
                self._NETWORK_SHA256_CACHE[stat_key] = network_sha256
        return (
            network_sha256,
            int(self._ROUTING_TOPOLOGY_SCHEMA_VERSION) * 2 + int(bool(include_center)),
            str(topology_version),
        )

    def _routing_topology_cache_path(self, cache_key):
        configured = self._client_config_value("routing_topology_cache_dir", None)
        if configured:
            cache_dir = os.path.abspath(os.path.expanduser(os.fspath(configured)))
        else:
            base = (
                os.environ.get("XDG_CACHE_HOME")
                or os.environ.get("LOCALAPPDATA")
                or os.path.join(os.path.expanduser("~"), ".cache")
            )
            cache_dir = os.path.join(base, "metsr-hpc", "routing")
        network_sha256, schema_version, topology_version = cache_key
        version_token = hashlib.sha256(topology_version.encode("utf-8")).hexdigest()[:16]
        filename = f"topology-v{schema_version}-{network_sha256}-{version_token}.json.gz"
        return os.path.join(cache_dir, filename)

    def _load_routing_topology_cache(
            self,
            topology_version,
            include_center=False,
            expected_count=None):
        if not self._routing_truthy(
                self._client_config_value("routing_topology_cache", True)):
            return None
        cache_key = self._routing_topology_cache_key(topology_version, include_center)
        if cache_key is None:
            return None
        with self._ROUTING_TOPOLOGY_CACHE_LOCK:
            memory_payload = self._ROUTING_TOPOLOGY_MEMORY_CACHE.get(cache_key)
        if memory_payload is not None:
            memory_records = memory_payload.get("records")
            if (
                    isinstance(memory_records, list)
                    and (
                        expected_count is None
                        or len(memory_records) == int(expected_count)
                    )):
                return memory_payload
            with self._ROUTING_TOPOLOGY_CACHE_LOCK:
                self._ROUTING_TOPOLOGY_MEMORY_CACHE.pop(cache_key, None)
        cache_path = self._routing_topology_cache_path(cache_key)
        try:
            with gzip.open(cache_path, "rt", encoding="utf-8") as cache_file:
                payload = json.load(cache_file)
            payload_key = (
                payload.get("networkSHA256"),
                int(payload.get("schemaVersion")),
                str(payload.get("topologyVersion")),
            )
            records = payload.get("records")
            if (
                    payload_key != cache_key
                    or not isinstance(records, list)
                    or (
                        expected_count is not None
                        and len(records) != int(expected_count)
                    )):
                return None
        except (AttributeError, EOFError, OSError, TypeError, ValueError, json.JSONDecodeError):
            return None
        with self._ROUTING_TOPOLOGY_CACHE_LOCK:
            self._ROUTING_TOPOLOGY_MEMORY_CACHE[cache_key] = payload
        return payload

    def _save_routing_topology_cache(self, topology_version, records, include_center=False):
        if not self._routing_truthy(
                self._client_config_value("routing_topology_cache", True)):
            return
        cache_key = self._routing_topology_cache_key(topology_version, include_center)
        if cache_key is None:
            return
        payload = {
            "networkSHA256": cache_key[0],
            "schemaVersion": cache_key[1],
            "topologyVersion": cache_key[2],
            # Records are deliberately static: no speed, travel time, energy,
            # flow, occupancy, metric version, or tick is persisted.
            "records": records,
        }
        with self._ROUTING_TOPOLOGY_CACHE_LOCK:
            self._ROUTING_TOPOLOGY_MEMORY_CACHE[cache_key] = payload
        cache_path = self._routing_topology_cache_path(cache_key)
        cache_dir = os.path.dirname(cache_path)
        temp_path = None
        try:
            os.makedirs(cache_dir, exist_ok=True)
            descriptor, temp_path = tempfile.mkstemp(
                prefix=".topology-",
                suffix=".tmp",
                dir=cache_dir,
            )
            with os.fdopen(descriptor, "wb") as raw_file:
                with gzip.GzipFile(fileobj=raw_file, mode="wb") as cache_file:
                    cache_file.write(json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"))
            os.replace(temp_path, cache_path)
            temp_path = None
        except OSError:
            pass
        finally:
            if temp_path is not None:
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    @classmethod
    def _static_topology_records(cls, records, schema=None):
        normalized = []
        schema = list(schema or [])
        positions = {name: index for index, name in enumerate(schema)}
        for raw in records:
            if isinstance(raw, dict):
                road_id = raw.get("roadId")
                if road_id is None:
                    road_id = raw.get("segmentId")
                downstream = raw.get("downstreamRoadId")
                if downstream is None:
                    downstream = cls._routing_downstream_from(raw, default=None)
                if downstream is None:
                    downstream = raw.get("downstreamIds", [])
                length = cls._routing_float(
                    raw, "length", "distance", "distance_m", default=0.0
                )
                center_x = raw.get("centerX")
                center_y = raw.get("centerY")
            elif isinstance(raw, (list, tuple)) and len(raw) >= 3:
                road_index = positions.get("roadId", 0)
                downstream_index = positions.get("downstreamRoadId", 1)
                length_index = positions.get("length", 2)
                road_id = raw[road_index]
                downstream = raw[downstream_index]
                try:
                    length = float(raw[length_index])
                except (TypeError, ValueError):
                    length = 0.0
                center_x = (
                    raw[positions["centerX"]]
                    if "centerX" in positions and positions["centerX"] < len(raw)
                    else None
                )
                center_y = (
                    raw[positions["centerY"]]
                    if "centerY" in positions and positions["centerY"] < len(raw)
                    else None
                )
            else:
                continue
            if road_id is None:
                continue
            record = {
                "segmentId": str(road_id),
                "downstreamIds": [
                    str(value) for value in (downstream or []) if value is not None
                ],
                "length": length,
            }
            if center_x is not None and center_y is not None:
                record["centerX"] = center_x
                record["centerY"] = center_y
            normalized.append(record)
        return normalized

    def _build_static_topology_graph(
            self,
            records,
            metadata,
            cache_hit=False,
            include_center=False):
        graph = nx.DiGraph()
        for record in records:
            attrs = {
                "distance": float(record.get("length", 0.0)),
                "length": float(record.get("length", 0.0)),
            }
            if "centerX" in record and "centerY" in record:
                attrs["center_x"] = record["centerX"]
                attrs["center_y"] = record["centerY"]
            graph.add_node(record["segmentId"], **attrs)
        for record in records:
            edge_attrs = {
                "distance": float(record.get("length", 0.0)),
                "length": float(record.get("length", 0.0)),
            }
            for downstream_id in record.get("downstreamIds", []):
                graph.add_edge(record["segmentId"], downstream_id, **edge_attrs)
        graph.graph.update({
            "edge_cost_road": "source",
            "snapshot_required": False,
            "topology_only": True,
            "topology_cache_hit": bool(cache_hit),
            "topology_schema_version": (
                int(self._ROUTING_TOPOLOGY_SCHEMA_VERSION) * 2 + int(bool(include_center))
            ),
            "topology_includes_center": bool(include_center),
        })
        if metadata.get("topologyVersion") is not None:
            graph.graph["topology_version"] = metadata["topologyVersion"]
        if metadata.get("tick") is not None:
            graph.graph["tick"] = metadata["tick"]
        cache_key = self._routing_topology_cache_key(metadata.get("topologyVersion") or "", include_center)
        if cache_key is not None:
            graph.graph["network_sha256"] = cache_key[0]
        return graph

    def query_routing_graph(
            self,
            batch_size=None,
            topology_only=False,
            use_cache=True,
            include_center=False):
        """Build and return the road-level NetworkX routing graph.

        The default batch comes from ``routing_graph_batch_size`` (5,000 when
        unset) and shrinks automatically when responses approach the
        WebSocket byte limit.

        ``topology_only=True`` omits all live metrics and caches only road IDs,
        downstream connections, lengths, and optional centers. Cache keys are
        the network-file SHA-256, client schema version, and simulator
        topology version. Full graphs expose the latest estimator fields as
        travel_time_p90, travel_time_confidence,
        travel_time_effective_sample_count, travel_time_sample_age_seconds,
        travel_time_live_vehicle_count, travel_time_stopped_fraction,
        travel_time_live_lower_bound, travel_time_live_mean_speed, and
        travel_time_estimate_source on nodes and source-road edges.
        """
        batch_size, _, _ = self._routing_batch_settings(batch_size)
        # Prefer compact topology pages when the simulator supports them.
        if topology_only and self._supports_feature("routingTopology"):
            probe = self.query_routing_topology(
                offset=0,
                limit=0,
                include_center=include_center,
                compact=True,
                legacy_fallback=True,
            )
            if not probe.get("legacyFallback"):
                if include_center:
                    probe_schema = set(probe.get("schema") or [])
                    if not {"centerX", "centerY"}.issubset(probe_schema):
                        raise RuntimeError(
                            "METS-R SIM routingTopology response omitted requested centers"
                        )
                topology_version = probe.get("topologyVersion")
                total = max(0, int(probe.get("total", 0)))
                if use_cache:
                    cached = self._load_routing_topology_cache(
                        topology_version,
                        include_center=include_center,
                        expected_count=total,
                    )
                    if cached is not None:
                        return self._build_static_topology_graph(
                            cached["records"],
                            probe,
                            cache_hit=True,
                            include_center=include_center,
                        )

                requested_batch, minimum_batch, target_bytes = self._routing_batch_settings(
                    batch_size
                )
                offset = 0
                current_batch = requested_batch
                records = []
                metadata = probe
                correlation_failures = 0
                while offset < total:
                    count = min(current_batch, total - offset)
                    try:
                        response = self.query_routing_topology(
                            offset=offset,
                            limit=count,
                            include_center=include_center,
                            compact=True,
                            legacy_fallback=False,
                        )
                    except (RuntimeError, TimeoutError) as exc:
                        if count <= minimum_batch or (
                            isinstance(exc, RuntimeError) and not self._routing_response_size_error(exc)
                        ):
                            raise
                        self._recover_routing_connection()
                        current_batch = max(minimum_batch, count // 2)
                        continue
                    try:
                        response_offset = int(response.get("offset"))
                        response_total = int(response.get("total"))
                    except (TypeError, ValueError):
                        response_offset = -1
                        response_total = -1
                    if response_offset != offset or response_total != total:
                        correlation_failures += 1
                        if correlation_failures > 1:
                            raise RuntimeError(
                                "METS-R SIM routingTopology page correlation failed: "
                                f"requested offset/total {offset}/{total}, received "
                                f"{response_offset}/{response_total}"
                            )
                        self._recover_routing_connection()
                        continue
                    correlation_failures = 0
                    if str(response.get("topologyVersion")) != str(topology_version):
                        raise RuntimeError(
                            "METS-R SIM topology changed while its graph was being loaded"
                        )
                    page_records = self._static_topology_records(
                        response.get("data", []),
                        schema=response.get("schema"),
                    )
                    records.extend(page_records)
                    received_count = int(response.get(
                        "count", len(response.get("data", []))
                    ))
                    if received_count <= 0:
                        raise RuntimeError(
                            f"METS-R SIM returned an empty topology page at offset {offset}"
                        )
                    if len(page_records) != received_count:
                        raise RuntimeError(
                            "METS-R SIM routingTopology count does not match "
                            f"its data length at offset {offset}"
                        )
                    offset += received_count
                    metadata = response
                    response_bytes = max(1, int(self._last_response_bytes))
                    if response_bytes > target_bytes and count > minimum_batch:
                        scaled = int(count * target_bytes * 0.85 / response_bytes)
                        current_batch = max(minimum_batch, min(count, scaled))
                self._last_routing_batch_size = current_batch
                if use_cache:
                    self._save_routing_topology_cache(
                        topology_version,
                        records,
                        include_center=include_center,
                    )
                graph = self._build_static_topology_graph(
                    records,
                    metadata,
                    cache_hit=False,
                    include_center=include_center,
                )
                graph.graph["routing_batch_size"] = current_batch
                return graph
        if topology_only and include_center:
            raise RuntimeError(
                "include_center requires a METS-R SIM with routingTopology support"
            )

        all_roads_res = self.query_road()
        if all_roads_res.get("status") == "error":
            raise RuntimeError(f"METS-R SIM rejected road query: {all_roads_res}")
        road_ids = all_roads_res.get("roadIds") or []
        topology_version = all_roads_res.get("topologyVersion")

        if topology_only and use_cache:
            cached = self._load_routing_topology_cache(
                topology_version,
                include_center=False,
                expected_count=len(road_ids),
            )
            if cached is not None:
                return self._build_static_topology_graph(
                    cached["records"],
                    all_roads_res,
                    cache_hit=True,
                    include_center=False,
                )

        road_records, metadata_response = self._query_road_records_adaptive(
            road_ids,
            batch_size=batch_size,
        )
        if not metadata_response:
            metadata_response = all_roads_res
        if topology_only:
            static_records = self._static_topology_records(road_records)
            if use_cache:
                self._save_routing_topology_cache(
                    metadata_response.get("topologyVersion", topology_version),
                    static_records,
                    include_center=False,
                )
            graph = self._build_static_topology_graph(
                static_records,
                metadata_response,
                cache_hit=False,
                include_center=False,
            )
            graph.graph["legacy_topology_fallback"] = True
            graph.graph["routing_batch_size"] = getattr(
                self, "_last_routing_batch_size", batch_size
            )
            return graph

        graph = nx.DiGraph()
        downstream_by_road = {}
        for road in road_records:
            if not isinstance(road, dict):
                continue
            road_id = road.get("segmentId")
            if road_id is None:
                continue
            road_id = str(road_id)
            graph.add_node(road_id, **self._routing_node_attrs_from(road))
            downstream_by_road[road_id] = self._routing_downstream_from(
                road, default=[]
            )
        for road_id, downstream_ids in downstream_by_road.items():
            edge_attrs = self._routing_edge_attrs_from(graph.nodes[road_id])
            for downstream_id in downstream_ids or []:
                graph.add_edge(road_id, downstream_id, **edge_attrs)

        self._apply_routing_graph_metadata(graph, metadata_response)
        graph.graph["snapshot_required"] = False
        graph.graph["topology_only"] = False
        graph.graph["routing_batch_size"] = getattr(
            self, "_last_routing_batch_size", batch_size
        )
        return graph

    def update_routing_graph(
            self,
            graph,
            update_response=None,
            include_topology=False,
            reload_on_snapshot_required=True,
            batch_size=None):
        """Apply ``routingGraphUpdates`` results to an existing graph.

        Normal update responses are metric-only. When SIM reports
        ``snapshotRequired``, the topology has changed and this method reloads a
        full graph unless ``reload_on_snapshot_required`` is false.
        ``include_topology`` is kept for compatibility and is used only if an
        update record explicitly carries downstream road IDs.
        """
        if update_response is None:
            update_response = self.query_routing_graph_updates()
        if update_response.get("status") == "error":
            raise RuntimeError(f"METS-R SIM rejected routingGraphUpdates: {update_response}")

        if self._routing_truthy(update_response.get("snapshotRequired", False)):
            if not reload_on_snapshot_required:
                self._apply_routing_graph_metadata(graph, update_response)
                raise RuntimeError("METS-R SIM requested a fresh routing graph snapshot")
            return self.query_routing_graph(
                batch_size=batch_size,
                topology_only=bool(graph.graph.get("topology_only", False)),
                include_center=bool(
                    graph.graph.get("topology_includes_center", False)
                ),
                use_cache=True,
            )

        removed_ids = update_response.get("removed", []) or update_response.get("REMOVED", []) or []
        for road_id in removed_ids:
            road_id = str(road_id)
            if road_id in graph:
                graph.remove_node(road_id)

        for road in update_response.get("data", []):
            if not isinstance(road, dict):
                continue
            road_id = road.get("segmentId")
            if road_id is None:
                continue
            road_id = str(road_id)
            status = str(road.get("status", "ok")).lower()
            if status in {"error", "REMOVED", "DELETE", "DELETED"}:
                if road_id in graph and status != "error":
                    graph.remove_node(road_id)
                continue

            previous = graph.nodes[road_id] if road_id in graph else {}
            graph.add_node(road_id, **self._routing_node_attrs_from(road, previous=previous))
            edge_attrs = self._routing_edge_attrs_from(graph.nodes[road_id])
            downstream_ids = self._routing_downstream_from(road, default=None)
            if downstream_ids is not None:
                graph.remove_edges_from(list(graph.out_edges(road_id)))
                for downstream_id in downstream_ids:
                    graph.add_edge(road_id, downstream_id, **edge_attrs)
            else:
                for _, downstream_id in list(graph.out_edges(road_id)):
                    graph.edges[road_id, downstream_id].update(edge_attrs)

        self._apply_routing_graph_metadata(graph, update_response)
        return graph
