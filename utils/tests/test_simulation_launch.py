"""Launcher regression contracts migrated from the legacy utility test suite.

Every process/container call is mocked. Tests exercise the owning modules so
mocking helper functions does not depend on the public util facade internals.
"""
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from utils import simulation_commands, simulation_handles, simulation_launch, simulation_setup


def _launcher_options(sim_dir):
    return SimpleNamespace(
        num_simulations=1,
        sim_dirs=[str(sim_dir)],
        sim_dir="/opt/METS-R_SIM/METS_R/",
        repast_plugin_dir="/opt/Eclipse/plugins/",
        java_path="/opt/jdk/bin/",
        java_options="-Xms1G -Xmx3G",
        verbose=False,
        preparation_timings=[{"preparation": 0.25}],
    )


class _FakeProcess:
    def __init__(self, pid=4321):
        self.pid = pid
        self._returncode = None
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait_calls = []

    def poll(self):
        return self._returncode

    def terminate(self):
        self.terminate_calls += 1

    def kill(self):
        self.kill_calls += 1

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        self._returncode = 0
        return 0


class OptimizedLauncherTests(unittest.TestCase):
    def test_docker_uses_argv_latest_image_resources_jvm_jfr_and_scoped_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            sim_dir = Path(directory)
            options = _launcher_options(sim_dir)
            options.docker_executable = "docker-test"
            options.docker_cpus = 3.5
            options.docker_memory = "6g"
            options.docker_cpuset_cpus = "2-5"
            options.docker_args = ["--security-opt", "no-new-privileges"]
            options.n_threads = 8
            options.n_partition = 4
            options.enable_jfr = True
            options.jfr_filename = "profiles/run_{index}.jfr"
            launch_result = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="container-123\n",
                stderr="",
            )
            stop_result = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="container-123\n",
                stderr="",
            )

            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(
                    simulation_launch.subprocess,
                    "run",
                    side_effect=[launch_result, stop_result],
                ) as run,
            ):
                handles = simulation_launch.run_simulation_in_docker(options)
                handle = handles[0]

                command = run.call_args_list[0].args[0]
                self.assertIsInstance(command, list)
                self.assertNotIn("shell", run.call_args_list[0].kwargs)
                self.assertEqual(command, handle.command)
                self.assertEqual(
                    command[:4],
                    ["docker-test", "run", "--pull=always", "-d"],
                )
                self.assertIn("--cpus", command)
                self.assertEqual(command[command.index("--cpus") + 1], "3.5")
                self.assertEqual(command[command.index("--memory") + 1], "6g")
                self.assertEqual(command[command.index("--cpuset-cpus") + 1], "2-5")
                self.assertIn("-Xms1G", command)
                self.assertIn("-Xmx3G", command)
                self.assertNotIn("-Xmx16G", command)
                self.assertTrue(
                    any(
                        item.startswith("-XX:StartFlightRecording=")
                        for item in command
                    )
                )
                self.assertEqual(
                    simulation_commands.DEFAULT_METSR_SIM_IMAGE,
                    "ennuilei/mets-r_sim:latest",
                )
                self.assertEqual(handle.image, simulation_commands.DEFAULT_METSR_SIM_IMAGE)
                self.assertNotIn("@sha256:", handle.image)
                self.assertLess(
                    command.index("--pull=always"),
                    command.index(handle.image),
                )
                self.assertEqual(handle.container_id, "container-123")
                self.assertIsNone(handle.process)
                self.assertEqual(handle.identifier, "container-123")
                self.assertEqual(handle.resources["threads"], 8)
                self.assertEqual(handle.resources["partitions"], 4)
                self.assertEqual(handle.timings["preparation"], 0.25)
                self.assertIsNotNone(handle.timings["launch"])
                self.assertIs(handles, options.launch_handles)
                self.assertIs(options.launch_timings[0], handle._timings)

                handles.cleanup(timeout=7)
                handles.cleanup(timeout=7)

            self.assertEqual(run.call_count, 2)
            self.assertEqual(
                run.call_args_list[1].args[0],
                ["docker-test", "stop", "--time", "7", "container-123"],
            )

    def test_docker_rejects_pull_policy_override(self):
        with tempfile.TemporaryDirectory() as directory:
            sim_dir = Path(directory)
            options = _launcher_options(sim_dir)
            options.docker_executable = "docker-test"
            for docker_args in (["--pull", "never"], ["--pull=never"]):
                with self.subTest(docker_args=docker_args):
                    options.docker_args = docker_args
                    with mock.patch.object(simulation_launch.subprocess, "run") as run:
                        with self.assertRaisesRegex(
                            ValueError,
                            "--pull=always",
                        ):
                            simulation_launch.run_simulation_in_docker(options)
                    run.assert_not_called()

    def test_docker_cleanup_failure_can_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            handle = simulation_handles.SimulationLaunchHandle(
                "docker",
                ["docker-test", "run"],
                directory,
                container_id="retry-container",
                docker_executable="docker-test",
            )
            failed = subprocess.CompletedProcess(
                args=[],
                returncode=1,
                stdout="",
                stderr="daemon unavailable",
            )
            succeeded = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="retry-container\n",
                stderr="",
            )

            with mock.patch.object(
                simulation_launch.subprocess,
                "run",
                side_effect=[failed, succeeded],
            ) as run:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "daemon unavailable",
                ):
                    handle.cleanup(timeout=2)
                self.assertFalse(handle.cleaned_up)

                handle.cleanup(timeout=2)
                self.assertTrue(handle.cleaned_up)

            self.assertEqual(run.call_count, 2)

    def test_launch_group_cleanup_attempts_all_handles(self):
        succeeds = mock.Mock()
        succeeds.identifier = "success"
        succeeds.cleanup.return_value = "stopped"
        fails = mock.Mock()
        fails.identifier = "failure"
        fails.cleanup.side_effect = RuntimeError("cannot stop")
        handles = simulation_handles.SimulationLaunchGroup([succeeds, fails])

        with self.assertRaisesRegex(
            RuntimeError,
            "Failed to clean up 1 METS-R launch resource",
        ):
            handles.cleanup(timeout=4)

        fails.cleanup.assert_called_once_with(timeout=4)
        succeeds.cleanup.assert_called_once_with(timeout=4)

    def test_windows_shell_args_preserve_legacy_option_paths(self):
        with mock.patch.object(simulation_commands.platform, "system", return_value="Windows"):
            args = simulation_commands._shell_args(
                r'-Dconfig="C:\Program Files\METS R\config.properties" -Xmx4G'
            )

        self.assertEqual(
            args,
            [r"-Dconfig=C:\Program Files\METS R\config.properties", "-Xmx4G"],
        )

    def test_absolute_jfr_filename_inside_run_is_container_relative(self):
        with tempfile.TemporaryDirectory() as directory:
            sim_dir = Path(directory) / "run"
            sim_dir.mkdir()
            absolute_jfr = sim_dir / "profiles" / "absolute.jfr"
            options = _launcher_options(sim_dir)
            options.enable_jfr = True
            options.jfr_filename = str(absolute_jfr)
            launch_result = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="container-jfr\n",
                stderr="",
            )

            with mock.patch.object(
                simulation_launch.subprocess,
                "run",
                return_value=launch_result,
            ) as run:
                handles = simulation_launch.run_simulation_in_docker(options)

            command = run.call_args.args[0]
            recording_argument = next(
                item
                for item in command
                if item.startswith("-XX:StartFlightRecording=")
            )
            self.assertIn("filename=profiles/absolute.jfr", recording_argument)
            self.assertNotIn(str(sim_dir.resolve()), recording_argument)
            self.assertEqual(
                handles[0].resources["jfr"]["filename"],
                "profiles/absolute.jfr",
            )
            self.assertTrue(absolute_jfr.parent.is_dir())

    def test_jfr_filename_outside_run_is_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sim_dir = root / "run"
            sim_dir.mkdir()
            outside_jfr = root / "outside.jfr"
            options = _launcher_options(sim_dir)
            options.enable_jfr = True
            options.jfr_filename = str(outside_jfr)

            with mock.patch.object(simulation_launch.subprocess, "run") as run:
                with self.assertRaisesRegex(
                    ValueError,
                    "jfr_filename must resolve inside simulation run directory",
                ):
                    simulation_launch.run_simulation_in_docker(options)

            run.assert_not_called()
            self.assertFalse(outside_jfr.exists())

    def test_relative_jfr_filename_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sim_dir = root / "run"
            sim_dir.mkdir()
            options = _launcher_options(sim_dir)
            options.enable_jfr = True
            options.jfr_filename = "../outside.jfr"

            with mock.patch.object(simulation_launch.subprocess, "run") as run:
                with self.assertRaisesRegex(
                    ValueError,
                    "jfr_filename must resolve inside simulation run directory",
                ):
                    simulation_launch.run_simulation_in_docker(options)

            run.assert_not_called()

    def test_jfr_options_cannot_override_scoped_filename(self):
        unsafe_options = (
            "-XX:StartFlightRecording=filename=../raw.jfr",
            "settings=profile,filename=../fragment.jfr",
            {"filename": "../dict.jfr", "settings": "profile"},
            ["settings=profile", "filename=../list.jfr"],
        )
        with tempfile.TemporaryDirectory() as directory:
            sim_dir = Path(directory)
            for unsafe in unsafe_options:
                with self.subTest(jfr_options=unsafe):
                    options = _launcher_options(sim_dir)
                    options.enable_jfr = True
                    options.jfr_options = unsafe
                    with mock.patch.object(simulation_launch.subprocess, "run") as run:
                        with self.assertRaisesRegex(
                            ValueError,
                            "use jfr_filename",
                        ):
                            simulation_launch.run_simulation_in_docker(options)
                    run.assert_not_called()

    def test_jfr_option_formats_preserve_fragments_and_recording_controls(self):
        formats = (
            " ,settings=profile,disk=true, ",
            ["settings=profile", "disk=true"],
            {"settings": "profile", "disk": True, "unused": None},
        )
        with tempfile.TemporaryDirectory() as directory:
            for fragments in formats:
                with self.subTest(fragments=fragments):
                    options = _launcher_options(directory)
                    options.jfr_options = fragments
                    options.jfr_duration = "30s"
                    options.jfr_unlock_commercial_features = True
                    with mock.patch.dict(os.environ, {}, clear=True):
                        command, recording = simulation_commands._build_batch_java_command(
                            options, 0, directory
                        )
                    self.assertIn("-XX:+UnlockCommercialFeatures", command)
                    self.assertIn(
                        "-XX:StartFlightRecording=filename=profiles/metsr_0.jfr,"
                        "dumponexit=true,duration=30s,settings=profile,disk=true",
                        command,
                    )
                    self.assertEqual(recording, {
                        "enabled": True, "filename": "profiles/metsr_0.jfr",
                    })

    def test_appcontainer_uses_argv_returns_process_handle_and_terminates_once(self):
        with tempfile.TemporaryDirectory() as directory:
            sim_dir = Path(directory)
            options = _launcher_options(sim_dir)
            options.verbose = True
            options.appcontainer_executable = "apptainer-test"
            options.appcontainer_image = "docker://example/sim@sha256:abc"
            options.appcontainer_args = ["--containall"]
            options.appcontainer_cpus = 2
            options.appcontainer_memory = "4g"
            process = _FakeProcess()

            with (
                mock.patch.object(simulation_launch.subprocess, "Popen", return_value=process) as popen,
                mock.patch("builtins.print"),
            ):
                handles = simulation_launch.run_simulation_in_appcontainer(options)
                handle = handles[0]
                command = popen.call_args.args[0]

                self.assertIsInstance(command, list)
                self.assertNotIn("shell", popen.call_args.kwargs)
                self.assertEqual(command[:3], ["apptainer-test", "exec", "--containall"])
                self.assertEqual(command[command.index("--cpus") + 1], "2")
                self.assertEqual(command[command.index("--memory") + 1], "4g")
                self.assertIn("docker://example/sim@sha256:abc", command)
                self.assertIs(handle.process, process)
                self.assertEqual(handle.identifier, 4321)
                self.assertEqual(handle.pid, 4321)
                self.assertEqual(handle.backend, "appcontainer")

                handle.cleanup(timeout=3)
                handle.cleanup(timeout=3)

            self.assertEqual(process.terminate_calls, 1)
            self.assertEqual(process.kill_calls, 0)
            self.assertEqual(process.wait_calls, [3])

    def test_appcontainer_defaults_to_latest_docker_image(self):
        with tempfile.TemporaryDirectory() as directory:
            sim_dir = Path(directory)
            options = _launcher_options(sim_dir)
            options.verbose = True
            options.appcontainer_executable = "apptainer-test"
            process = _FakeProcess()

            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(simulation_launch.subprocess, "Popen", return_value=process) as popen,
                mock.patch("builtins.print"),
            ):
                handles = simulation_launch.run_simulation_in_appcontainer(options)

            handle = handles[0]
            command = popen.call_args.args[0]
            self.assertEqual(
                simulation_commands.DEFAULT_METSR_SIM_APPCONTAINER_IMAGE,
                "docker://ennuilei/mets-r_sim:latest",
            )
            self.assertEqual(handle.image, simulation_commands.DEFAULT_METSR_SIM_APPCONTAINER_IMAGE)
            self.assertIn(handle.image, command)
            self.assertNotIn("@sha256:", handle.image)

            handles.cleanup()

    def test_thin_preparation_writes_only_mutable_files_and_retargets_data_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "data"
            source.mkdir()
            (source / "log4j.properties").write_text("log=true\n", encoding="utf-8")
            (source / "Data.properties.Template").write_text(
                "NETWORK_LISTEN_PORT = 1\n"
                "NETWORK_FILE = data/NYC/facility/road/nyc.net.xml\n"
                "N_THREADS = 1\n"
                "N_PARTITION = 1\n",
                encoding="utf-8",
            )
            immutable_file = source / "large-immutable-input.bin"
            immutable_file.write_bytes(b"immutable")
            run_dir = root / "run"
            options = SimpleNamespace(
                name="thin",
                random_seeds=[42],
                num_simulations=1,
                metsr_port=[61279],
                template="Template",
                thin_run=True,
                thin_run_data_source=str(source),
                thin_run_data_target="/opt/metsr-inputs",
                n_threads=12,
                n_partition=4,
            )

            old_cwd = os.getcwd()
            try:
                os.chdir(root)
                with mock.patch.object(simulation_setup, "get_sim_dir", return_value=str(run_dir)):
                    roots = simulation_setup.prepare_sim_dirs(options)
            finally:
                os.chdir(old_cwd)

            generated = run_dir / "data" / "Data.properties"
            properties = generated.read_text(encoding="utf-8")
            manifest = json.loads(
                (run_dir / "data" / ".metsr_hpc_inputs.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(roots, [str(run_dir)])
            self.assertTrue(generated.is_file())
            self.assertTrue((run_dir / "logs").is_dir())
            self.assertEqual(
                manifest,
                {
                    "schema_version": 1,
                    "host_data_source": str(source.resolve()),
                    "container_data_target": "/opt/metsr-inputs",
                },
            )
            self.assertFalse((run_dir / "data" / immutable_file.name).exists())
            self.assertIn(
                "NETWORK_FILE = /opt/metsr-inputs/NYC/facility/road/nyc.net.xml",
                properties,
            )
            self.assertIn("N_THREADS = 12", properties)
            self.assertIn("N_PARTITION = 4", properties)
            self.assertEqual(options.thin_run_data_source, str(source.resolve()))
            self.assertEqual(options.thin_run_data_target, "/opt/metsr-inputs")
            self.assertTrue(options.preparation_timings[0]["thin_run"])
            self.assertGreaterEqual(options.preparation_time, 0.0)

    def test_thin_preparation_rejects_stale_nonempty_data_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "data"
            source.mkdir()
            (source / "log4j.properties").write_text("log=true\n", encoding="utf-8")
            (source / "Data.properties.Template").write_text(
                "NETWORK_LISTEN_PORT = 1\n",
                encoding="utf-8",
            )
            run_dir = root / "run"
            run_data = run_dir / "data"
            run_data.mkdir(parents=True)
            run_log4j = run_dir / "log4j.properties"
            run_log4j.write_text("keep=true\n", encoding="utf-8")
            stale_file = run_data / "immutable-stale.bin"
            stale_file.write_bytes(b"do-not-delete")
            options = SimpleNamespace(
                name="thin-stale",
                random_seeds=[42],
                num_simulations=1,
                metsr_port=[61280],
                template="Template",
                thin_run=True,
                thin_run_data_source=str(source),
                thin_run_data_target="/opt/metsr-inputs",
            )

            old_cwd = os.getcwd()
            try:
                os.chdir(root)
                with mock.patch.object(
                    simulation_setup,
                    "get_sim_dir",
                    return_value=str(run_dir),
                ):
                    with self.assertRaisesRegex(
                        FileExistsError,
                        "Refusing to reuse thin-run data directory",
                    ):
                        simulation_setup.prepare_sim_dirs(options)
            finally:
                os.chdir(old_cwd)

            self.assertEqual(stale_file.read_bytes(), b"do-not-delete")
            self.assertEqual(
                run_log4j.read_text(encoding="utf-8"),
                "keep=true\n",
            )
            self.assertFalse((run_dir / "logs").exists())
            self.assertFalse(
                (run_data / ".metsr_hpc_inputs.json").exists()
            )

    def test_thread_and_partition_lists_are_indexed_per_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "data"
            source.mkdir()
            (source / "log4j.properties").write_text("log=true\n", encoding="utf-8")
            (source / "Data.properties.Template").write_text(
                "NETWORK_LISTEN_PORT = 1\n"
                "N_THREADS = 1\n"
                "N_PARTITION = 1\n",
                encoding="utf-8",
            )
            run_dirs = [root / "run-0", root / "run-1"]
            options = SimpleNamespace(
                name="thin-multi",
                random_seeds=[41, 42],
                num_simulations=2,
                metsr_port=[61281, 61282],
                template="Template",
                thin_run=True,
                thin_run_data_source=str(source),
                thin_run_data_target="/opt/metsr-inputs",
                n_threads=[4, 8],
                n_partition=[2, 4],
            )

            old_cwd = os.getcwd()
            try:
                os.chdir(root)
                with mock.patch.object(
                    simulation_setup,
                    "get_sim_dir",
                    side_effect=[str(run_dir) for run_dir in run_dirs],
                ):
                    simulation_setup.prepare_sim_dirs(options)
            finally:
                os.chdir(old_cwd)

            properties = [
                (run_dir / "data" / "Data.properties").read_text(
                    encoding="utf-8"
                )
                for run_dir in run_dirs
            ]
            self.assertIn("N_THREADS = 4", properties[0])
            self.assertIn("N_PARTITION = 2", properties[0])
            self.assertIn("N_THREADS = 8", properties[1])
            self.assertIn("N_PARTITION = 4", properties[1])

    def test_property_file_aliases_preserve_extensions_and_explicit_overrides(self):
        template = (
            "ZONES_SHAPEFILE = original\n"
            "ZONES_CSV = original\n"
            "CHARGER_SHAPEFILE = original\n"
            "CHARGER_CSV = original\n"
            "RH_DEMAND_SHARABLE = false\n"
            "MixedCaseProperty = original\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            (source / "Data.properties.Template").write_text(template, encoding="utf-8")
            destination = Path(directory) / "run"
            destination.mkdir()
            options = {
                "zone_file": "zones.SHP",
                "zones_csv": "explicit.csv",
                "charging_station_file": "chargers",
                "rh_wait_file": "wait.csv",
                "mixed_case_property": 7,
            }
            for configured in (options, SimpleNamespace(**options)):
                with self.subTest(options_type=type(configured).__name__):
                    simulation_setup.modify_property_file(
                        configured, str(source), str(destination), 61200, 0, "Template"
                    )
                    generated = (destination / "Data.properties").read_text(encoding="utf-8")
                    self.assertTrue(generated.startswith(
                        "ZONES_SHAPEFILE = zones.SHP\n"
                        "ZONES_CSV = explicit.csv\n"
                        "CHARGER_SHAPEFILE = chargers.shp\n"
                        "CHARGER_CSV = chargers.csv\n"
                        "RH_DEMAND_SHARABLE = true\n"
                        "MixedCaseProperty = 7\n"
                    ))

    def test_docker_thin_run_adds_read_only_static_input_mount(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sim_dir = root / "run"
            (sim_dir / "data").mkdir(parents=True)
            (sim_dir / "data" / "Data.properties").write_text(
                "NETWORK_FILE = /opt/static/network.xml\n",
                encoding="utf-8",
            )
            static_data = root / "static"
            static_data.mkdir()
            options = _launcher_options(sim_dir)
            options.thin_run = True
            options.thin_run_data_source = str(static_data)
            options.thin_run_data_target = "/opt/static"
            result = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="thin-container\n",
                stderr="",
            )

            with mock.patch.object(simulation_launch.subprocess, "run", return_value=result) as run:
                simulation_launch.run_simulation_in_docker(options)

            command = run.call_args.args[0]
            mounts = [
                command[index + 1]
                for index, value in enumerate(command)
                if value == "--mount"
            ]
            self.assertEqual(len(mounts), 2)
            self.assertIn(
                (
                    f"type=bind,source={static_data.resolve()},target=/opt/static,"
                    "readonly"
                ),
                mounts,
            )

    def test_handle_extracts_connection_network_and_fleet_timings_from_log(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "mets_r.log"
            log_path.write_text(
                "100 Connection object created.\n"
                "600 Connected to controller\n"
                "800 Building subcontexts\n"
                "2800 City initialized!\n"
                "3000 VehicleContext creation\n"
                "4500 Total EV buses generated\n",
                encoding="utf-8",
            )
            handle = simulation_handles.SimulationLaunchHandle(
                "process",
                ["java"],
                directory,
                process=_FakeProcess(),
                log_path=str(log_path),
            )

            timings = handle.timings

            self.assertEqual(timings["connection"], 0.5)
            self.assertEqual(timings["network_load"], 2.2)
            self.assertEqual(timings["fleet_spawn"], 1.5)


if __name__ == "__main__":
    unittest.main()
