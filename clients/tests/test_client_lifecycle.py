"""Connection ownership and public compatibility, without a simulator server."""

import importlib
import inspect
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest import mock

try:
    import websockets.sync.client  # noqa: F401
except ModuleNotFoundError:
    websockets_module = types.ModuleType("websockets")
    sync_module = types.ModuleType("websockets.sync")
    socket_module = types.ModuleType("websockets.sync.client")
    socket_module.connect = None
    websockets_module.sync = sync_module
    sync_module.client = socket_module
    sys.modules.setdefault("websockets", websockets_module)
    sys.modules.setdefault("websockets.sync", sync_module)
    sys.modules.setdefault("websockets.sync.client", socket_module)

client_module = importlib.import_module("clients.METSRClient")
visualization_module = importlib.import_module("utils.visualization.client")
METSRClient = client_module.METSRClient


def ready_socket(message=None):
    message = message or {"messageType": "ready", "status": "ok"}
    socket = mock.Mock()
    socket.recv.return_value = json.dumps(message)
    return socket


class ClientLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.socket = ready_socket()
        connect = mock.patch.object(client_module, "connect", return_value=self.socket)
        self.connect = connect.start()
        self.addCleanup(connect.stop)
        register = mock.patch.object(client_module, "register_metsr_client")
        self.register = register.start()
        self.addCleanup(register.stop)
        output = mock.patch("builtins.print")
        output.start()
        self.addCleanup(output.stop)

    def create(self, **kwargs):
        client = METSRClient("localhost", 4000, **kwargs)
        self.addCleanup(client.close)
        return client

    def test_constructor_waits_for_ready_before_registration(self):
        events = []
        self.connect.side_effect = lambda *args, **kwargs: events.append("connect") or self.socket
        self.socket.recv.side_effect = lambda **kwargs: (
            events.append("ready") or json.dumps({"messageType": "ready", "status": "ok"})
        )

        def register(client):
            self.assertEqual(client.state, "connected")
            self.assertEqual(client.current_tick, 0)
            self.assertIsNotNone(client.lock)
            events.append("registered")

        self.register.side_effect = register
        client = self.create(
            connection_open_timeout=0.2,
            config=SimpleNamespace(websocket_max_size=4096, startup_ready_timeout=2.0),
        )
        self.assertEqual(events, ["connect", "ready", "registered"])
        self.connect.assert_called_once_with(
            "ws://localhost:4000", max_size=4096,
            ping_interval=None, ping_timeout=None, open_timeout=0.2,
        )
        self.socket.recv.assert_called_once_with(timeout=2.0)
        self.register.assert_called_once_with(client)
        self.assertIs(client.ws, self.socket)
        self.assertIs(client.profiling, client.rpc_profile)
        self.assertGreaterEqual(client.startup_timings["connection_seconds"], 0)
        self.socket.close.assert_not_called()

    def test_late_connection_accepts_heartbeat_and_preserves_reported_tick(self):
        self.socket.recv.return_value = json.dumps({
            "messageType": "step", "status": "ok", "tick": 42,
        })
        client = self.create()
        self.assertEqual(client.current_tick, 42)
        self.assertEqual(client.state, "connected")
        self.register.assert_called_once_with(client)

    def test_unexpected_handshake_closes_socket_and_does_not_register(self):
        self.socket.recv.return_value = json.dumps({
            "messageType": "vehicle", "status": "ok", "data": [],
        })
        client = METSRClient.__new__(METSRClient)
        with self.assertRaisesRegex(RuntimeError, "Expected METS-R SIM ready"):
            METSRClient.__init__(client, "localhost", 4000)
        self.assertEqual(client.state, "failed")
        self.assertIsNone(client.ws)
        self.socket.close.assert_called_once_with()
        self.register.assert_not_called()

    def test_handshake_timeout_closes_socket_and_does_not_register(self):
        client = METSRClient.__new__(METSRClient)
        with mock.patch.object(METSRClient, "receive_msg", return_value=None):
            with self.assertRaisesRegex(TimeoutError, "did not report ready or step"):
                METSRClient.__init__(client, "localhost", 4000)
        self.assertEqual(client.state, "failed")
        self.assertIsNone(client.ws)
        self.socket.close.assert_called_once_with()
        self.register.assert_not_called()

    def test_connection_failure_stops_without_registration_or_handshake(self):
        self.connect.side_effect = OSError("connection refused")
        client = METSRClient.__new__(METSRClient)
        with self.assertRaisesRegex(RuntimeError, "Could not connect to METS-R SIM"):
            METSRClient.__init__(client, "localhost", 4000, max_connection_wait=0)
        self.assertEqual(client.state, "failed")
        self.assertIsNone(getattr(client, "ws", None))
        self.connect.assert_called_once()
        self.socket.recv.assert_not_called()
        self.register.assert_not_called()

    def test_connection_retry_registers_only_after_successful_handshake(self):
        self.connect.side_effect = [OSError("not listening yet"), self.socket]
        with mock.patch.object(client_module.time, "sleep") as sleep:
            client = self.create(connection_retry_interval=0)
        self.assertEqual(self.connect.call_count, 2)
        sleep.assert_called_once_with(0)
        self.socket.recv.assert_called_once()
        self.register.assert_called_once_with(client)

    def test_reconnect_clears_connection_caches_and_waits_for_new_readiness(self):
        client = self.create()
        first_profile = client.rpc_profile
        client._cached_fatal_log_error = "old failed process"
        client._last_fatal_log_check = float("inf")
        client._capabilities_cache = {"runEpoch": 1}
        client._optimized_api_available = True
        client._feature_support.update(advanceAndSnapshot=True)
        self.socket.close()
        client.ws = None
        second_socket = ready_socket({"messageType": "step", "status": "ok", "tick": 17})
        self.connect.return_value = second_socket
        client._connect(connection_open_timeout=0.3)
        self.assertIs(client.ws, second_socket)
        self.assertEqual(client.state, "connected")
        self.assertEqual(client.current_tick, 17)
        self.assertIsNone(client._cached_fatal_log_error)
        self.assertIsNone(client._capabilities_cache)
        self.assertIsNone(client._optimized_api_available)
        self.assertEqual(client._feature_support, {})
        self.assertIs(client.rpc_profile, first_profile)
        second_socket.recv.assert_called_once()
        self.register.assert_called_once_with(client)
        self.assertEqual(self.connect.call_args.kwargs["open_timeout"], 0.3)

    def test_close_releases_socket_and_both_visualization_servers(self):
        client = self.create()
        client.viz_server = object()
        client.viz_stream_server = object()
        client.stop_offline_viz = mock.Mock(side_effect=lambda: setattr(client, "viz_server", None))
        client.stop_viz_stream = mock.Mock(side_effect=lambda: setattr(client, "viz_stream_server", None))
        with mock.patch("clients.METSRClient.unregister_metsr_client") as unregister:
            client.close()
        self.assertEqual(client.state, "closed")
        self.assertIsNone(client.ws)
        self.socket.close.assert_called_once_with()
        client.stop_offline_viz.assert_called_once_with()
        client.stop_viz_stream.assert_called_once_with()
        unregister.assert_called_once_with(client)

    def test_restart_ends_old_run_then_waits_for_new_ready_before_restoring_visualization(self):
        client = self.create(sim_index=1, connection_open_timeout=0.6)
        events = []
        client.current_tick = 50
        client._legacy_command_results["old-command"] = {"status": "ok"}
        client._capabilities_cache = {"runEpoch": 1}
        client._feature_support["fieldMasks"] = True
        client._attack_vehicle_keys.add("old-vehicle")
        client._fatal_log_error = mock.Mock(return_value=None)
        client.viz_stream_server = object()
        client.viz_stream_start_kwargs = {"server_port": 8765, "host": "127.0.0.1"}
        client.viz_server = object()
        client.offline_viz_start_kwargs = {"server_port": 8000, "prefer_binary": False}
        client.stop_viz_stream = mock.Mock(side_effect=lambda: setattr(client, "viz_stream_server", None))
        client.stop_offline_viz = mock.Mock(side_effect=lambda: setattr(client, "viz_server", None))

        def record_request(payload):
            self.assertEqual(json.loads(payload), {"messageType": "end"})
            events.append("send end")

        self.socket.send.side_effect = record_request
        self.socket.recv.side_effect = lambda **kwargs: (
            events.append("end acknowledged") or json.dumps({"messageType": "end", "status": "ok"})
        )
        self.socket.close.side_effect = lambda: events.append("close old socket")
        def port_released(address, timeout):
            self.assertEqual(address, ("127.0.0.1", 4000))
            self.assertEqual(timeout, 0.25)
            events.append("port released")
            raise ConnectionRefusedError

        second_socket = ready_socket()
        second_socket.recv.side_effect = lambda **kwargs: (
            events.append("new ready") or json.dumps({"messageType": "ready", "status": "ok"})
        )
        self.connect.side_effect = lambda *args, **kwargs: events.append("connect new") or second_socket

        def launch(config):
            self.assertIsNone(client.ws)
            self.assertEqual(client.state, "closed")
            events.append("launch new")

        def restore(name):
            def run(**kwargs):
                self.assertIs(client.ws, second_socket)
                self.assertEqual(client.current_tick, 0)
                self.assertEqual(client._legacy_command_results, {})
                self.assertEqual(client._attack_vehicle_keys, set())
                self.assertIsNone(client._capabilities_cache)
                self.assertEqual(client._feature_support, {})
                events.append(name)
            return run

        client.start_viz = mock.Mock(side_effect=restore("restore live"))
        client.start_offline_viz = mock.Mock(side_effect=restore("restore offline"))
        config = SimpleNamespace(metsr_host="new-server", ports=[4100, 4200], sim_step_size=0.1)
        signature = json.dumps({"sim_step_size": 0.1})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new-config.json"
            path.write_text(signature, encoding="utf-8")
            with (
                mock.patch.object(client_module, "read_run_config", return_value=config) as read_config,
                mock.patch.object(client_module, "prepare_sim_dirs", side_effect=lambda _: events.append("prepare") or ["run-0", "run-1"]) as prepare,
                mock.patch.object(client_module, "run_simulation_in_docker", side_effect=launch) as start,
                mock.patch.object(client_module.socket, "create_connection", side_effect=port_released) as probe,
            ):
                self.assertIsNone(client.reset(path))
            read_config.assert_called_once_with(str(path.resolve()))

        self.assertEqual(events, [
            "prepare", "send end", "end acknowledged", "close old socket",
            "port released", "launch new", "connect new", "new ready",
            "restore live", "restore offline",
        ])
        prepare.assert_called_once_with(config)
        start.assert_called_once_with(config)
        probe.assert_called_once_with(("127.0.0.1", 4000), timeout=0.25)
        self.assertEqual(client.uri, "ws://new-server:4200")
        self.assertEqual(client.sim_folder, "run-1")
        self.assertIs(client.config, config)
        self.assertEqual(json.loads(client.config_signature), json.loads(signature))
        self.assertEqual(self.connect.call_args.args, ("ws://new-server:4200",))
        self.assertEqual(self.connect.call_args.kwargs["open_timeout"], 0.6)
        client.start_viz.assert_called_once_with(server_port=8765, host="127.0.0.1")
        client.start_offline_viz.assert_called_once_with(server_port=8000, prefer_binary=False)
        self.register.assert_called_once_with(client)

    def test_failed_restart_launch_keeps_old_socket_closed_and_does_not_reconnect(self):
        client = self.create()
        self.socket.recv.return_value = json.dumps({"messageType": "end", "status": "ok"})
        client.start_viz = mock.Mock()
        client.start_offline_viz = mock.Mock()
        config = SimpleNamespace(metsr_host="new-server", ports=[4100])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new-config.json"
            path.write_text("{}", encoding="utf-8")
            with (
                mock.patch.object(client_module, "read_run_config", return_value=config),
                mock.patch.object(client_module, "prepare_sim_dirs", return_value=["new-run"]),
                mock.patch.object(client_module, "run_simulation_in_docker", side_effect=RuntimeError("launch failed")),
                mock.patch.object(client_module.socket, "create_connection", side_effect=ConnectionRefusedError),
            ):
                with self.assertRaisesRegex(RuntimeError, "launch failed"):
                    client.reset(path)
        self.assertIsNone(client.ws)
        self.assertEqual(client.state, "closed")
        self.socket.close.assert_called_once_with()
        self.connect.assert_called_once()  # Only the original constructor connected.
        client.start_viz.assert_not_called()
        client.start_offline_viz.assert_not_called()

    def test_reset_with_unchanged_configuration_resets_without_relaunching(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text('{"sim_step_size": 0.1}', encoding="utf-8")
            client = self.create(config_json=path)
            client.current_tick = 40
            client._legacy_command_results["old-command"] = {"status": "ok"}
            client._capabilities_cache = {"runEpoch": 1}
            client._attack_vehicle_keys.add("old-vehicle")
            self.socket.recv.return_value = json.dumps({
                "messageType": "reset", "status": "ok", "tick": 0,
            })
            config = SimpleNamespace(sim_step_size=0.1)
            with (
                mock.patch.object(client_module, "read_run_config", return_value=config),
                mock.patch.object(client_module, "prepare_sim_dirs") as prepare,
                mock.patch.object(client_module, "run_simulation_in_docker") as launch,
            ):
                self.assertIsNone(client.reset(path))

        self.assertEqual(json.loads(self.socket.send.call_args.args[0]), {"messageType": "reset"})
        self.assertEqual(client.current_tick, 0)
        self.assertEqual(client._legacy_command_results, {})
        self.assertIsNone(client._capabilities_cache)
        self.assertEqual(client._attack_vehicle_keys, set())
        self.assertIs(client.config, config)
        self.assertIs(client.ws, self.socket)
        self.socket.close.assert_not_called()
        prepare.assert_not_called()
        launch.assert_not_called()
        self.connect.assert_called_once()

    def test_live_visualization_preserves_raw_restart_arguments_before_normalization(self):
        client = self.create()
        supplied = {
            "server_port": "9876", "host": "127.0.0.2", "tick_interval": 0,
            "coord_scale": 0, "link_snapshot_interval": 0,
            "link_batch_size": 0, "facility_batch_size": 0,
            "include_links": 1, "transform_coords": 1,
            "road_id_dictionary": "query", "initial_x": 2, "initial_y": -3,
        }
        expected = {
            name: parameter.default
            for name, parameter in inspect.signature(client.start_viz).parameters.items()
        }
        expected.update(supplied)
        server_module = types.ModuleType("websockets.sync.server")
        server_module.serve = mock.Mock(return_value=mock.Mock())
        self.enterContext(mock.patch.dict(sys.modules, {server_module.__name__: server_module}))
        thread = self.enterContext(mock.patch.object(visualization_module.threading, "Thread"))
        self.enterContext(mock.patch.object(client, "_wait_for_viz_stream_server"))
        roads = self.enterContext(mock.patch.object(client, "query_road", return_value={"roadIds": ["r1"]}))
        self.addCleanup(client.stop_viz_stream)

        result = client.start_viz(**supplied)

        self.assertEqual(client.viz_stream_start_kwargs, expected)
        self.assertEqual(result["port"], 9876)
        self.assertEqual(client.viz_stream_options["road_id_dictionary"], ["r1"])
        for key in ("tick_interval", "coord_scale", "link_snapshot_interval",
                    "link_batch_size", "facility_batch_size"):
            self.assertEqual(client.viz_stream_options[key], 1)
            self.assertEqual(client.viz_stream_start_kwargs[key], 0)
        for key in ("include_links", "transform_coords"):
            self.assertIs(client.viz_stream_options[key], True)
            self.assertIs(type(client.viz_stream_start_kwargs[key]), int)
        roads.assert_called_once_with()
        thread.return_value.start.assert_called_once_with()
        self.assertEqual(server_module.serve.call_args.args[1:], ("127.0.0.2", 9876))

    def test_offline_visualization_preserves_requested_directory_resolution_for_restart(self):
        client = self.create()
        supplied = {"server_port": 8088, "prefer_binary": False, "wait_seconds": 4}
        expected = {
            name: parameter.default
            for name, parameter in inspect.signature(client.start_offline_viz).parameters.items()
        }
        expected.update(supplied)
        latest = self.enterContext(mock.patch.object(
            client, "latest_trajectory_output_dir", return_value="saved-trajectory",
        ))
        self.enterContext(mock.patch.object(visualization_module, "_trajectory_format_name", return_value="binary"))
        server = self.enterContext(mock.patch.object(
            visualization_module, "run_visualization_server", return_value=(mock.Mock(), mock.Mock()),
        ))
        self.enterContext(mock.patch.object(visualization_module, "stop_visualization_server"))
        probe = self.enterContext(mock.patch.object(visualization_module.socket, "create_connection"))
        self.addCleanup(client.stop_offline_viz)

        result = client.start_offline_viz(**supplied)

        self.assertEqual(client.offline_viz_start_kwargs, expected)
        self.assertIsNone(client.offline_viz_start_kwargs["trajectory_output_dir"])
        self.assertEqual(result["directory"], "saved-trajectory")
        latest.assert_called_once_with(trajectory_output_dir=None, prefer_binary=False, wait_seconds=4)
        server.assert_called_once_with("saved-trajectory", 8088)
        probe.assert_called_once_with(("127.0.0.1", 8088), timeout=0.5)

    def test_query_consumes_stale_snapshot_response_before_returning_its_own_response(self):
        client = self.create()
        response = {"messageType": "taxi", "status": "ok", "data": [{"taxiId": 4}]}
        stale = {"messageType": "advanceAndSnapshot", "status": "ok", "finalTick": 8}
        events = []
        responses = iter([stale, response])
        self.socket.send.side_effect = lambda payload: events.append(("send", json.loads(payload)))

        def receive(**kwargs):
            item = next(responses)
            events.append(("receive", item["messageType"]))
            return json.dumps(item)

        self.socket.recv.side_effect = receive
        self.assertEqual(client.query_taxi([4]), response)
        self.assertEqual(events, [
            ("send", {"messageType": "taxi", "data": [4]}),
            ("receive", "advanceAndSnapshot"), ("receive", "taxi"),
        ])
        self.assertEqual(client.current_tick, 8)

    def test_legacy_clock_patch_still_controls_profile_measurement(self):
        client = self.create()
        client.current_tick = 9
        with mock.patch("clients.METSRClient.time.perf_counter", return_value=12.5):
            client._record_server_tick_time(started_at=10, starting_tick=5)
        profile = client.get_rpc_profile()
        self.assertEqual(profile["server_ticks"], 4)
        self.assertEqual(profile["server_tick_time"], 2.5)

    def test_constructor_uses_json_snapshot_for_transport_readiness_and_step_size(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps({
                "websocket_max_size": 8192, "startup_ready_timeout": 1.5,
                "sim_step_size": 0.2,
            }), encoding="utf-8")
            client = self.create(config_json=path)
        self.assertEqual(self.connect.call_args.kwargs["max_size"], 8192)
        self.socket.recv.assert_called_once_with(timeout=1.5)
        self.assertEqual(client._configured_sim_step_size(), 0.2)
        self.assertEqual(client.config_json, str(path.resolve()))
        self.assertIsNone(client.config)

    def test_remembering_without_overrides_preserves_explicit_config_precedence(self):
        config = SimpleNamespace(sim_step_size=0.4)
        client = self.create(config=config)
        client._remember_config()
        self.assertIs(client.config, config)
        self.assertEqual(client._client_config_values, {"sim_step_size": 0.4})
        client._remember_config(config_signature='{"sim_step_size": 0.1}')
        self.assertIs(client.config, config)
        self.assertEqual(client._client_config_values, {"sim_step_size": 0.1})
        self.assertEqual(client._configured_sim_step_size(), 0.4)

    def test_bounded_retries_keep_one_deadline_per_attempt_and_one_command_id(self):
        client = self.create()
        request = {"messageType": "advanceAndSnapshot", "data": {"tickCount": 1}}
        rejected = {"messageType": "advanceAndSnapshot", "status": "ok", "commandId": "stale"}
        accepted = {"messageType": "advanceAndSnapshot", "status": "ok", "commandId": "current"}
        client.send_msg = mock.Mock()
        client.receive_msg = mock.Mock(side_effect=[rejected, None, accepted])
        matcher = mock.Mock(side_effect=lambda response: response is accepted)
        with mock.patch.object(client_module.time, "monotonic", side_effect=[100, 100.25, 101.25, 200, 200.5]):
            result = client.send_receive_msg(request, True, max_attempts=2, timeout=2, response_matcher=matcher)
        self.assertIs(result, accepted)
        self.assertEqual([call.kwargs["timeout"] for call in client.receive_msg.call_args_list], [1.75, 0.75, 1.5])
        self.assertTrue(all(call.kwargs["print_timeout"] is False for call in client.receive_msg.call_args_list))
        self.assertTrue(all(call.kwargs["waiting_forever"] is False for call in client.receive_msg.call_args_list))
        self.assertEqual(matcher.call_args_list, [mock.call(rejected), mock.call(accepted)])
        self.assertEqual(client.send_msg.call_count, 2)
        first, second = [call.args[0] for call in client.send_msg.call_args_list]
        self.assertEqual(first, second)
        self.assertTrue(first["data"]["commandId"])
        self.assertNotIn("commandId", request["data"])

    def test_bounded_retry_exhaustion_releases_lock_and_reports_attempts(self):
        client = self.create()
        client.send_msg = mock.Mock()
        client.receive_msg = mock.Mock(return_value=None)
        with mock.patch.object(client_module.time, "monotonic", side_effect=[0, 0, 10, 10]):
            with self.assertRaisesRegex(TimeoutError, "after 2 attempts"):
                client.send_receive_msg({"messageType": "taxi"}, True, max_attempts=2, timeout=1)
        self.assertEqual(client.send_msg.call_count, 2)
        self.assertEqual(client.receive_msg.call_count, 2)
        self.assertTrue(client.lock.acquire(blocking=False))
        client.lock.release()

    def test_unbounded_matching_receives_without_resending_rejected_none(self):
        client = self.create()
        rejected = {"messageType": "taxi", "status": "ok", "data": []}
        accepted = {"messageType": "taxi", "status": "ok", "data": [4]}
        client.send_msg = mock.Mock()
        client.receive_msg = mock.Mock(side_effect=[None, rejected, accepted])
        matcher = mock.Mock(side_effect=lambda response: response is accepted)
        with mock.patch.object(client_module.time, "monotonic") as clock:
            result = client.send_receive_msg(
                {"messageType": "taxi"}, True, max_attempts=-1, timeout=0.05,
                return_errors=True, response_matcher=matcher,
            )
        self.assertIs(result, accepted)
        client.send_msg.assert_called_once_with({"messageType": "taxi"})
        self.assertEqual(matcher.call_args_list, [mock.call(None), mock.call(rejected), mock.call(accepted)])
        self.assertEqual(client.receive_msg.call_args_list, [mock.call(
            ignore_heartbeats=True, waiting_forever=True, timeout=0.05, return_errors=True,
        )] * 3)
        clock.assert_not_called()

    def test_unbounded_default_matcher_resends_after_none(self):
        client = self.create()
        accepted = {"messageType": "taxi", "status": "ok", "data": []}
        client.send_msg = mock.Mock()
        client.receive_msg = mock.Mock(side_effect=[None, accepted])
        result = client.send_receive_msg({"messageType": "taxi"}, True, max_attempts=0)
        self.assertIs(result, accepted)
        self.assertEqual(client.send_msg.call_count, 2)
        self.assertEqual(client.receive_msg.call_count, 2)

    def test_keyboard_interrupt_returns_none_and_releases_request_lock(self):
        client = self.create()
        client.send_msg = mock.Mock()
        client.receive_msg = mock.Mock(side_effect=KeyboardInterrupt)
        self.assertIsNone(client.send_receive_msg({"messageType": "taxi"}, True))
        client.send_msg.assert_called_once()
        self.assertTrue(client.lock.acquire(blocking=False))
        client.lock.release()


class ClientCompatibilityTests(unittest.TestCase):
    def test_core_client_workflows_are_defined_explicitly_on_the_client_class(self):
        for name in (
            "__init__", "_connect", "send_receive_msg", "reset", "close",
            "_client_config_value", "_remember_config", "get_rpc_profile",
            "list_apis",
        ):
            with self.subTest(method=name):
                self.assertIn(name, METSRClient.__dict__)
                self.assertEqual(getattr(METSRClient, name).__module__, "clients.METSRClient")

    def test_canonical_utility_helpers_preserve_conversion_and_visualization_behavior(self):
        from utils.primitives import str_list_mapper_gen
        from utils.visualization import protocol

        self.assertEqual(str_list_mapper_gen(int)(["1", "2"]), [1, 2])
        self.assertEqual(str_list_mapper_gen(float)(["1.25"]), [1.25])
        self.assertTrue(protocol._viz_stream_record({"x": 0, "y": 1}))
        self.assertFalse(protocol._viz_stream_record({"x": 0}))
        self.assertEqual(protocol._viz_int("12.5"), 12)
        self.assertEqual(protocol._viz_float("invalid", default=2), 2)
        record = {"vehicleId": 4, "vehicleClass": 3, "x": 0, "y": 1}
        groups = protocol._viz_group_vehicle_records([record])
        self.assertEqual(groups["ev_private"], [record])

    def test_catalog_is_complete_and_preserves_signatures_topics_and_aliases(self):
        with mock.patch.object(client_module, "connect", side_effect=AssertionError("must remain offline")):
            catalog = METSRClient.list_apis()
        public_names = [name for name in dir(METSRClient)
                        if not name.startswith("_") and callable(getattr(METSRClient, name))]
        self.assertEqual([entry["name"] for entry in catalog], public_names)
        entries = {entry["name"]: entry for entry in catalog}
        self.assertTrue(all(entry["description"].strip() for entry in catalog))
        self.assertEqual(entries["query_taxi"]["signature"], "(id=None)")
        self.assertEqual(entries["reposition_taxi"]["signature"], "(vehID, zoneID=None, roadID=None)")
        self.assertEqual(entries["query_bus"]["category"], "query")
        self.assertEqual(entries["query_bus"]["topic"], "public_vehicles")
        self.assertEqual(entries["reposition_taxi"]["topic"], "taxi")
        self.assertEqual(entries["reset"]["category"], "control")
        self.assertEqual(entries["reset"]["topic"], "simulation")
        self.assertEqual(entries["reset"]["module"], "clients.METSRClient")
        for name in ("render", "start_viz", "stop_viz", "start_offline_viz", "get_trajectory_summary"):
            self.assertEqual(entries[name]["topic"], "visualization", name)
            self.assertEqual(entries[name]["module"], "utils.visualization.client", name)
            self.assertIs(getattr(METSRClient, name), getattr(visualization_module.VisualizationMixin, name))
        self.assertEqual(METSRClient.list_apis(category="query"),
                         [entry for entry in catalog if entry["category"] == "query"])
        self.assertEqual(METSRClient.list_apis(topic="missing-topic"), [])
        with self.assertRaises(ValueError):
            METSRClient.list_apis(category="missing-category")
        for alias, canonical in (("queryBoundaryVeh", "query_boundary_vehicle"),
                                 ("initialize_coSim_vehicle", "initialize_cosim_vehicle")):
            self.assertIs(getattr(METSRClient, alias), getattr(METSRClient, canonical))
            for field in ("signature", "description", "category", "topic"):
                self.assertEqual(entries[alias][field], entries[canonical][field])

    def test_existing_query_and_control_calls_keep_wire_contract(self):
        client = METSRClient.__new__(METSRClient)
        query_response = {"messageType": "taxi", "status": "ok", "data": []}
        control_response = {"messageType": "repositionTaxi", "status": "ok", "data": []}
        client.send_receive_msg = mock.Mock(side_effect=[query_response, control_response])
        self.assertIs(client.query_taxi([4, 5]), query_response)
        self.assertIs(client.reposition_taxi([4, 5], zoneID=3), control_response)
        self.assertEqual(client.send_receive_msg.call_args_list, [
            mock.call({"messageType": "taxi", "data": [4, 5]}, ignore_heartbeats=True),
            mock.call({"messageType": "repositionTaxi", "data": [
                {"vehicleId": 4, "zoneId": 3}, {"vehicleId": 5, "zoneId": 3},
            ]}, ignore_heartbeats=True),
        ])


if __name__ == "__main__":
    unittest.main()
