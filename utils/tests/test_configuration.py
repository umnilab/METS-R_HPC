"""Configuration contracts independent of launchers and the public util facade."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from utils import configuration


class ConfigurationTests(unittest.TestCase):
    def test_parent_paths_are_relative_and_child_fields_replace_parent_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child_dir = root / "profiles"
            child_dir.mkdir()
            (root / "base.json").write_text(json.dumps({
                "num_simulations": 2, "random_seeds": [1, 2], "name": "parent",
                "nested": {"from_parent": True},
            }), encoding="utf-8")
            child = child_dir / "child.json"
            child.write_text(json.dumps({
                "parent_config": "../base.json", "name": "child", "nested": {"selected": 7},
            }), encoding="utf-8")
            options = configuration.read_run_config(child)
        self.assertEqual(options.name, "child")
        self.assertEqual(options.random_seeds, [1, 2])
        self.assertEqual(options.nested, {"selected": 7})
        self.assertFalse(hasattr(options, "parent_config"))

    def test_seed_count_mismatch_retains_the_public_exit_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory) / "invalid.json"
            filename.write_text('{"num_simulations": 2, "random_seeds": [1]}', encoding="utf-8")
            with patch("builtins.print"), self.assertRaises(SystemExit) as failure:
                configuration.read_run_config(filename)
        self.assertEqual(failure.exception.code, -1)

    def test_property_reader_skips_comments_and_preserves_embedded_equals(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory) / "Data.properties"
            filename.write_text("# comment\n\nA = first\ninvalid\nURL = a=b\nA = last\n", encoding="utf-8")
            self.assertEqual(configuration._read_property_values(filename), {"A": "last", "URL": "a=b"})
            self.assertEqual(configuration._read_property_values(Path(directory) / "missing"), {})

    def test_launcher_options_prefer_explicit_values_then_environment_then_default(self):
        with patch.dict(os.environ, {"METSR_TEST_VALUE": "environment"}):
            for options in ({"selected": 0}, SimpleNamespace(selected=0)):
                self.assertEqual(configuration._launcher_value(options, ("selected",), "METSR_TEST_VALUE", 9), 0)
            self.assertEqual(configuration._launcher_value({}, ("selected",), "METSR_TEST_VALUE", 9), "environment")
        self.assertEqual(configuration._launcher_value({}, ("selected",), default=9), 9)

    def test_free_port_probe_records_and_rechecks_candidates_without_real_sockets(self):
        probes = [Mock(), Mock(), Mock(), Mock()]
        probes[0].getsockname.return_value = ("localhost", 61200)
        probes[1].getsockname.return_value = ("localhost", 61201)
        options = SimpleNamespace(ports=[99])
        with patch.object(configuration.socket, "socket", side_effect=probes), patch.object(configuration.time, "sleep") as sleep:
            configuration.find_free_ports(options, 2)
        self.assertEqual(options.ports, [61200, 61201])
        probes[2].bind.assert_called_once_with(("", 61200))
        probes[3].bind.assert_called_once_with(("", 61201))
        for probe in probes:
            probe.close.assert_called_once_with()
        sleep.assert_called_once_with(1)

    def test_implementation_modules_do_not_import_the_util_facade(self):
        script = """
import importlib.abc
import sys
class NoFacade(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == 'utils.util':
            raise AssertionError('Implementation imported the util facade')
sys.meta_path.insert(0, NoFacade())
from utils import configuration, simulation_setup, simulation_commands, simulation_handles, simulation_launch
assert configuration._coerce_bool('false') is False
assert simulation_commands._shell_args(['-Xmx1G']) == ['-Xmx1G']
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
