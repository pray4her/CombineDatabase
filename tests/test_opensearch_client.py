from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from search_app.opensearch_client import load_dotenv_file, load_settings


class OpenSearchClientSettingsTests(unittest.TestCase):
    def test_load_settings_reads_env_file_defaults(self) -> None:
        keys = [
            "OPENSEARCH_ENDPOINT",
            "OPENSEARCH_USERNAME",
            "OPENSEARCH_PASSWORD",
            "OPENSEARCH_INITIAL_ADMIN_PASSWORD",
            "OPENSEARCH_INSECURE",
        ]
        snapshot = {key: os.environ.get(key) for key in keys}
        try:
            for key in keys:
                os.environ.pop(key, None)

            with tempfile.TemporaryDirectory() as tmp_dir:
                env_path = Path(tmp_dir) / ".env"
                env_path.write_text(
                    "OPENSEARCH_INITIAL_ADMIN_PASSWORD=demo-password\n",
                    encoding="utf-8",
                )
                load_dotenv_file(env_path)
                settings = load_settings()

            self.assertEqual(settings.endpoint, "https://localhost:9200")
            self.assertEqual(settings.username, "admin")
            self.assertEqual(settings.password, "demo-password")
            self.assertTrue(settings.insecure)
        finally:
            for key, value in snapshot.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()

