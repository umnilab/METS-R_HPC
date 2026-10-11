"""Exercise generated bridge files and runner arguments without simulators."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


BRIDGE = Path(__file__).resolve().parents[1]
if os.name == "nt":
    git_bash = Path(os.environ.get("ProgramFiles", "C:/Program Files"), "Git/bin/bash.exe")
    BASH = str(git_bash) if git_bash.is_file() else None
else:
    BASH = shutil.which("bash")


@unittest.skipUnless(BASH, "Bash is required to exercise bridge scripts")
class Simu5GScriptsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="metsr bridge scripts ")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        sources = [
            BRIDGE / "generate_sim5g_files.sh",
            BRIDGE / "run_sim5g_uu.sh",
            BRIDGE / "run_sim5g_pc5.sh",
            BRIDGE / "MetsrVeinsBridge.ned",
            *BRIDGE.glob("sim5g/*.template"),
            *BRIDGE.glob("sim5g/*.xml"),
        ]
        for source in sources:
            destination = self.root / source.relative_to(BRIDGE)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(source.read_text(encoding="utf-8").encode("utf-8"))
        (self.root / "check_sim5g_env.sh").write_text("exit 0\n")
        (self.root / "out").mkdir()
        (self.root / "out/libmetsr_veins_bridge_simu5g.so").touch()
        binary_dir = self.root / "bin"
        binary_dir.mkdir()
        runner = binary_dir / "opp_run"
        runner.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@" > invocation.txt\n')
        runner.chmod(0o755)
        self.env = {
            key: value for key, value in os.environ.items()
            if not key.startswith(("METSR_", "GENERATED_"))
        }
        self.env.update({
            "PATH": str(binary_dir) + os.pathsep + os.environ.get("PATH", ""),
            "INET_HOME": "/fake/inet", "SIMU5G_HOME": "/fake/simu5g",
        })

    def test_generated_files_match_templates_and_requested_backends(self):
        for backends in (("uu",), ("pc5",), ("uu", "pc5")):
            with self.subTest(backends=backends):
                output = "generated " + " ".join(backends)
                subprocess.run(
                    [BASH, "generate_sim5g_files.sh", output, *backends],
                    cwd=self.root, env=self.env, check=True, capture_output=True,
                )
                expected = {"MetsrVeinsBridge.ned": "MetsrVeinsBridge.ned"}
                for app in ("MetsrBsmUuApp", "MetsrBsmPc5App", "MetsrExternalMobility"):
                    expected[f"metsr/veinsbridge/sim5g/{app}.ned"] = f"sim5g/{app}.ned.template"
                for backend in backends:
                    network, routing = {
                        "uu": ("Sim5gCellularUuBridgeNetwork", "demo.xml"),
                        "pc5": ("Sim5gCv2xPc5BridgeNetwork", "pc5.xml"),
                    }[backend]
                    expected[f"{network}.ned"] = f"sim5g/{network}.ned.template"
                    expected[f"omnetpp-sim5g-{backend}.ini"] = f"sim5g/omnetpp-sim5g-{backend}.ini.template"
                    expected[f"sim5g/{routing}"] = f"sim5g/{routing}"
                generated = self.root / output
                self.assertEqual(
                    {str(p.relative_to(generated).as_posix()) for p in generated.rglob("*") if p.is_file()},
                    set(expected),
                )
                for filename, source in expected.items():
                    self.assertEqual(
                        (generated / filename).read_bytes(), (self.root / source).read_bytes(),
                        filename,
                    )

    def test_runners_preserve_defaults_and_overrides(self):
        for backend in ("uu", "pc5"):
            for override in (False, True):
                with self.subTest(backend=backend, override=override):
                    env = dict(self.env)
                    if override:
                        env.update({
                            "GENERATED_NED_ROOT": "generated custom",
                            "GENERATED_INI": "custom scenario.ini",
                            "METSR_SIMU5G_NUM_VEHICLES": "7", "METSR_BRIDGE_PORT": "9123",
                        })
                    subprocess.run(
                        [BASH, f"run_sim5g_{backend}.sh"], cwd=self.root,
                        env=env, check=True, capture_output=True,
                    )
                    args = (self.root / "invocation.txt").read_text().splitlines()
                    generated = "generated custom" if override else ".generated/sim5g-ned"
                    self.assertEqual(args, [
                        "-u", "Cmdenv", "-n",
                        f"{generated}:/fake/simu5g/src:/fake/simu5g/simulations:/fake/inet/src:/fake/inet/examples",
                        "-l", "/fake/inet/src/INET", "-l", "/fake/simu5g/src/simu5g",
                        "-l", "out/metsr_veins_bridge_simu5g", "-c", "General",
                        *([f"--*.bridge.bridgePort={9123 if override else 9099}"] if backend == "pc5" else []),
                        f"--*.numVehicles={7 if override else 64}",
                        "custom scenario.ini" if override else f"{generated}/omnetpp-sim5g-{backend}.ini",
                    ])

    def test_invalid_settings_stop_before_launch(self):
        for backend, setting, value in (
            ("uu", "METSR_SIMU5G_NUM_VEHICLES", "0"),
            ("pc5", "METSR_SIMU5G_NUM_VEHICLES", "bad"),
            ("pc5", "METSR_BRIDGE_PORT", "65536"),
        ):
            with self.subTest(backend=backend, setting=setting):
                result = subprocess.run(
                    [BASH, f"run_sim5g_{backend}.sh"], cwd=self.root,
                    env={**self.env, setting: value}, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn(setting, result.stderr)
                self.assertFalse((self.root / "invocation.txt").exists())


if __name__ == "__main__":
    unittest.main()
