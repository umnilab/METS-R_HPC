"""Visualization wire and lifetime contracts, without simulator or network servers."""

import importlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import weakref

from utils.visualization import client as visualization_client, protocol, registry, servers, trajectories


def client_class(test_case):
    """Import the client only for its behavior tests, with an offline SDK stub."""
    try:
        importlib.import_module("websockets.sync.client")
    except ModuleNotFoundError:
        modules = {name: ModuleType(name) for name in (
            "websockets", "websockets.sync", "websockets.sync.client",
        )}
        modules["websockets"].sync = modules["websockets.sync"]
        modules["websockets.sync"].client = modules["websockets.sync.client"]
        modules["websockets.sync.client"].connect = Mock()
        test_case.enterContext(patch.dict(sys.modules, modules))
    return importlib.import_module("clients.METSRClient").METSRClient


class VisualizationProtocolTests(unittest.TestCase):
    def test_empty_frame_preserves_binary_header_and_manifest_contract(self):
        manifest = protocol._viz_manifest(
            ["road-1"], 100000, 3.25, -4.5, 2, 10,
            zone_dictionary=[7], charging_station_dictionary=[8],
        )
        packet, vehicles, links, zones, stations = protocol._viz_chunk(
            [], 42, 100000, 3.25, -4.5, 2, 10,
        )
        header = struct.unpack_from(">4siiddii", packet)
        self.assertEqual(header, (b"MRTB", 8, 100000, 3.25, -4.5, 2, 10))
        self.assertEqual(struct.unpack_from(">i", packet, 36), (42,))
        self.assertEqual((vehicles, links, zones, stations), (0, 0, 0, 0))
        self.assertEqual(manifest["version"], header[1])
        self.assertEqual(manifest["chunkMagic"], header[0].decode("ascii"))
        self.assertEqual(manifest["sparseFrameGroups"], ["zone", "chargingStation"])
        self.assertEqual(manifest["roadIdDictionary"], ["road-1"])
        self.assertEqual(manifest["zoneDictionary"], [7])

    def test_origin_uses_properties_only_when_both_coordinates_are_unspecified(self):
        with tempfile.TemporaryDirectory() as temporary:
            data = Path(temporary) / "data"
            data.mkdir()
            (data / "Data.properties").write_text("INITIAL_X=12.5\nINITIAL_Y=-3\n")
            self.assertEqual(protocol._viz_resolve_origin(temporary), (12.5, -3.0))
            self.assertEqual(protocol._viz_resolve_origin(temporary, 1, 0), (1.0, 0.0))

    def test_modules_import_without_legacy_utility_or_optional_servers(self):
        script = """
import importlib.abc
import sys

class RejectIntegrationImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == 'utils.util' or fullname.split('.')[0] in {'clients', 'websockets', 'kafka'}:
            raise AssertionError('Unexpected import: ' + fullname)

sys.meta_path.insert(0, RejectIntegrationImports())
from utils.visualization import protocol, servers, trajectories, registry
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class StreamTests(unittest.TestCase):
    def stream(self):
        cls = client_class(self)
        client = cls.__new__(cls)
        client.viz_stream_server = object()
        client.viz_stream_manifest = protocol._viz_manifest([], 100, 0, 0, 1, 1)
        client.viz_stream_options = {
            "include_links": False, "include_zones": False, "include_charging_stations": False,
            "transform_coords": False, "include_public": True, "include_private": True,
            "vehicle_ids": None, "private_veh": False, "public_vehicle_ids": None,
            "private_vehicle_ids": None, "batch_size": 1000, "coord_scale": 100,
            "initial_x": 0, "initial_y": 0, "tick_interval": 1,
            "link_snapshot_interval": 1, "road_id_index": {},
        }
        client.viz_stream_chunk_counter = 0
        client.viz_stream_last_tick = 15
        client.viz_stream_last_active_road_ids = set()
        client.viz_stream_lock = threading.Lock()
        client._attack_vehicle_keys = set()
        client.viz_stream_port = 8765
        client.viz_stream_host = "localhost"
        websocket = Mock()
        client.viz_stream_clients = [websocket]
        client.query_tick = Mock(return_value=3)
        client.query_on_road_vehicles = Mock(return_value={"publicVehicleIds": [9]})
        client.query_cosim_vehicle = Mock(return_value={"data": []})
        client.query_vehicle = Mock(return_value={"data": [{
            "vehicleId": 9, "vehicleClass": 0, "x": 1.25, "y": 2.0, "speed": 5.0,
        }]})
        return client, websocket

    def test_render_sends_reset_manifest_metadata_then_binary_when_tick_rewinds(self):
        client, websocket = self.stream()
        result = client.render(client_wait_timeout=0)
        messages = [call.args[0] for call in websocket.send.call_args_list]
        self.assertEqual([json.loads(message)["type"] for message in messages[:3]],
                         ["reset", "manifest", "chunkMeta"])
        self.assertIsInstance(messages[3], bytes)
        self.assertEqual(messages[3][:4], b"MRTB")
        self.assertEqual(result["vehicle_count"], 1)
        self.assertEqual(result["client_count"], 1)
        self.assertEqual(client.viz_stream_last_tick, 3)
        client.query_tick.assert_called_once_with()
        client.query_vehicle.assert_called_once_with(id=[9], private_veh=[False], transform_coords=False)

    def test_disconnected_stream_client_is_removed_and_render_reports_failure(self):
        client, websocket = self.stream()
        websocket.send.side_effect = OSError("disconnected")
        with self.assertRaisesRegex(RuntimeError, "disconnected before the frame was sent"):
            client.render(client_wait_timeout=0)
        self.assertEqual(client.viz_stream_clients, [])
        self.assertEqual(client.viz_stream_last_tick, 15)

    def test_render_with_no_browser_reports_url_before_querying_simulation(self):
        client, _ = self.stream()
        client.viz_stream_clients = []
        with self.assertRaisesRegex(RuntimeError, "ws://localhost:8765"):
            client.render(client_wait_timeout=0)
        client.query_tick.assert_not_called()
        client.query_vehicle.assert_not_called()

    def test_render_combines_fleet_and_cosim_actors_and_keeps_attack_markers(self):
        client, _ = self.stream()
        client._attack_vehicle_keys = {(False, "11"), (False, "gone"), (True, "12")}
        client.query_cosim_vehicle.return_value = {"data": [
            {"vehicleId": 9, "isPrivate": False},
            {"vehicleId": 11, "isPrivate": False},
            {"vehicleId": 12, "isPrivate": True},
            {"vehicleId": -1}, {"status": "error", "vehicleId": 13},
        ]}
        client.query_vehicle.side_effect = lambda id, **kwargs: {"data": [
            {"vehicleId": vehicle_id, "vehicleClass": 0, "x": 1, "y": 2}
            for vehicle_id in id
        ]}
        with patch.object(visualization_client, "_viz_chunk", wraps=protocol._viz_chunk) as encode:
            result = client.render(client_wait_timeout=0)
        records = encode.call_args.args[0]
        self.assertEqual([record["vehicleId"] for record in records], [12, 9, 11])
        self.assertEqual([record["_viz_private_veh"] for record in records], [True, False, False])
        self.assertEqual([record["_viz_attack_vehicle"] for record in records], [True, False, True])
        self.assertEqual(client._attack_vehicle_keys, {(False, "11"), (True, "12")})
        self.assertEqual(result["vehicle_count"], 3)

    def test_render_clears_inactive_links_and_queries_facilities_in_batches(self):
        client, _ = self.stream()
        client.viz_stream_options.update({
            "include_links": True, "include_zones": True, "include_charging_stations": True,
            "link_batch_size": 1, "facility_batch_size": 1,
            "zone_dictionary": [], "charging_station_dictionary": [8, 9],
            "road_id_index": {"r1": 0, "r2": 1},
        })
        client.viz_stream_last_active_road_ids = {"r2"}
        client.query_active_roads = Mock(return_value={"roadIds": ["r1"]})
        client.query_road = Mock(side_effect=[
            {"data": [{"segmentId": "r1", "vehicleCount": 1}]}, {"data": []},
        ])
        client.query_zone = Mock(side_effect=[
            {"zoneIds": [7, None]}, {"data": [{"zoneId": 7, "x": 0, "y": 0}]},
        ])
        client.query_chargingStation = Mock(side_effect=[
            {"status": "error"}, {"data": [{"chargingStationId": 9, "x": 0, "y": 0}, None]},
        ])
        with patch.object(visualization_client, "_viz_chunk", wraps=protocol._viz_chunk) as encode:
            client.render(client_wait_timeout=0)
        payload = encode.call_args.kwargs
        self.assertEqual(payload["link_records"][1], {
            "segmentId": "r2", "vehicleCount": 0, "speed": 0.0, "flow": 0,
            "energyConsumed": 0.0, "parkingCapacity": 0, "parkedVehicleCount": 0,
        })
        self.assertEqual([record["zoneId"] for record in payload["zone_records"]], [7])
        self.assertEqual([record["chargingStationId"] for record in payload["charging_station_records"]], [9])
        self.assertEqual(client.viz_stream_last_active_road_ids, {"r1"})
        client.query_on_road_vehicles.assert_called_once_with(roadID=["r1"])
        self.assertEqual([call.kwargs for call in client.query_zone.call_args_list], [{}, {"id": [7]}])


class VisualizationLifetimeTests(unittest.TestCase):
    def test_public_file_server_wrappers_share_the_canonical_registry(self):
        from utils import util

        httpd = Mock()
        thread = Mock(httpd=httpd)
        thread.is_alive.return_value = False
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(registry, "_VISUALIZATION_SERVER_REGISTRY", []):
                with patch.object(servers, "start_cors_http_server", return_value=thread) as start:
                    with patch("builtins.print"):
                        event, returned_thread = util.run_visualization_server(temporary, 8087)
                self.assertIs(returned_thread, thread)
                start.assert_called_once_with(temporary, event, 8087)
                entry, = registry._VISUALIZATION_SERVER_REGISTRY
                self.assertIs(entry["server_thread"], thread)
                self.assertIs(entry["stop_event"], event)
                records = util.stop_all_visualization_servers(verbose=False, join_timeout=0.2)
                self.assertEqual(registry._VISUALIZATION_SERVER_REGISTRY, [])
        self.assertTrue(event.is_set())
        httpd.shutdown.assert_called_once_with()
        thread.join.assert_called_once_with(timeout=0.2)
        self.assertEqual(records[0]["port"], 8087)

    def test_public_client_server_cleanup_sees_clients_registered_by_metsr_client(self):
        from utils import util

        cls = client_class(self)
        with patch.object(registry, "_METSR_CLIENT_REGISTRY", weakref.WeakSet()):
            with patch.object(cls, "_connect"):
                client = cls("localhost", 4000)
            self.assertIn(client, registry._METSR_CLIENT_REGISTRY)
            client.viz_stream_server = object()
            client.stop_viz = Mock()
            records = util.stop_all_metsr_client_servers(verbose=False)
            client.stop_viz.assert_called_once_with(verbose=False)
            self.assertEqual(len(records), 1)
            self.assertTrue(records[0]["had_stream_server"])
            self.assertEqual(records[0]["returncode"], 0)
            util.unregister_metsr_client(client)
            self.assertNotIn(client, registry._METSR_CLIENT_REGISTRY)

    def test_registry_bulk_stop_uses_server_cleanup_and_unregisters_thread(self):
        event = threading.Event()
        server = Mock()
        thread = Mock(httpd=server)
        thread.is_alive.return_value = False
        registry._register_visualization_server(event, thread, port=8080)
        self.addCleanup(registry._unregister_visualization_server, thread)
        with patch.object(registry, "_VISUALIZATION_SERVER_REGISTRY", [{
            "stop_event": event, "server_thread": thread, "port": 8080, "directory": None,
        }]):
            result = registry.stop_all_visualization_servers(verbose=False, join_timeout=0.1)
            self.assertEqual(registry._VISUALIZATION_SERVER_REGISTRY, [])
        self.assertTrue(event.is_set())
        server.shutdown.assert_called_once_with()
        thread.join.assert_called_once_with(timeout=0.1)
        self.assertEqual(result[0]["returncode"], 0)

    def test_start_from_config_resolves_binary_output_and_returns_owned_stop(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "trajectory_output" / "run-1"
            output.mkdir(parents=True)
            (output / "manifest.json").write_text(json.dumps({"format": "binary", "version": 8}))
            config = SimpleNamespace(sim_folder=temporary)
            event, thread = object(), object()
            with patch.object(servers, "run_visualization_server", return_value=(event, thread)) as start:
                with patch("builtins.print"):
                    handle = servers.start_visualization_server_from_config(config, server_port=8123)
            self.assertEqual(handle.directory, str(output))
            start.assert_called_once_with(str(output), 8123)
            with patch.object(servers, "stop_visualization_server") as stop:
                handle.stop()
            stop.assert_called_once_with(event, thread, 8123)

    def test_binary_preference_survives_a_newer_json_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary, json_output = root / "binary", root / "json"
            binary.mkdir()
            json_output.mkdir()
            (binary / "manifest.json").write_text("{}")
            (json_output / "frame.json").write_text("{}")
            os.utime(binary, (1, 1))
            os.utime(json_output, (2, 2))
            self.assertEqual(trajectories._latest_trajectory_directory(temporary), str(binary))
            self.assertEqual(trajectories._latest_trajectory_directory(temporary, prefer_binary=False),
                             str(json_output))


if __name__ == "__main__":
    unittest.main()
