"""Dashboard asset preparation and offline cache fallback without network access."""

import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from utils.cosim_support import prepare_local_metsr_vis


class DashboardAssetTests(unittest.TestCase):
    def setUp(self):
        self.image = "a" * 32 + ".png"
        self.icon = "b" * 32 + ".svg"
        self.files = {
            "./": b'<script src="index.js"></script><link href="style.css">',
            "index.js": f'"ws://localhost:8765";n.e(10);n.e(2);"{self.image}"'.encode(),
            "style.css": f'url("{self.image}")'.encode(),
            "2.bundle.js": f'"{self.icon}"'.encode(),
            "10.bundle.js": b"// extra chunk",
            self.image: b"image data",
            self.icon: b"svg data",
            "index.js.LICENSE.txt": b"license",
        }

    def response(self, request, timeout):
        filename = request.full_url.removeprefix("https://viz.invalid/") or "./"
        data = self.files[filename]
        if isinstance(data, Exception):
            raise data
        response = io.BytesIO(data)
        response.headers = SimpleNamespace(get_content_charset=lambda: "utf-8")
        return response

    def test_preparation_copies_lazy_assets_and_survives_offline_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("urllib.request.urlopen", side_effect=self.response):
                result = prepare_local_metsr_vis(directory, "https://viz.invalid/", "ws://local:9999")
            assets = Path(directory, "metsr_vis")
            self.assertIn("ws://local:9999", (assets / "index.js").read_text())
            self.assertEqual((assets / self.image).read_bytes(), b"image data")
            self.assertEqual((assets / self.icon).read_bytes(), b"svg data")
            self.assertEqual(result["resource_info"], {
                "chunks": 2, "assets": 2, "total": 4, "mirrored": 4, "missing": [],
            })
            with patch("urllib.request.urlopen", side_effect=OSError("offline")):
                cached = prepare_local_metsr_vis(directory, "https://viz.invalid/", "ws://local:9999")
            self.assertEqual(cached, result)

    def test_missing_uncached_asset_is_reported_without_losing_other_assets(self):
        self.files[self.icon] = OSError("asset unavailable")
        with tempfile.TemporaryDirectory() as directory:
            with patch("urllib.request.urlopen", side_effect=self.response):
                result = prepare_local_metsr_vis(directory, "https://viz.invalid/", "ws://local:9999")
            self.assertEqual(result["resource_info"]["mirrored"], 3)
            self.assertEqual(result["resource_info"]["missing"], [
                {"file": self.icon, "error": "asset unavailable"},
            ])
            self.assertTrue(Path(directory, "metsr_vis", "index.html").is_file())


if __name__ == "__main__":
    unittest.main()
