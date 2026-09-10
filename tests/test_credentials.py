import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from meme_radar.credentials import load_credentials, load_env_file


class CredentialTests(unittest.TestCase):
    def test_loads_mode_0600_without_exposing_values(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / ".env"
            path.write_text(
                "BITQUERY_TOKEN=primary\n"
                "BITQUERY_TOKEN_STANDBY=standby\n"
                "GOPLUS_STANDBY_ENABLED=0\n",
                encoding="utf-8",
            )
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            values = load_credentials(path)
            self.assertEqual("primary", values.bitquery_token)
            self.assertEqual("standby", values.bitquery_token_standby)
            self.assertFalse(values.goplus_standby_enabled)

    def test_rejects_group_readable_file(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / ".env"
            path.write_text("BITQUERY_TOKEN=value\n", encoding="utf-8")
            path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)
            with self.assertRaises(PermissionError):
                load_env_file(path)

    def test_rejects_unknown_and_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / ".env"
            path.write_text("UNKNOWN_KEY=value\n", encoding="utf-8")
            path.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "unknown env key"):
                load_env_file(path)
            path.write_text(
                "BITQUERY_TOKEN=one\nBITQUERY_TOKEN=two\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate env key"):
                load_env_file(path)

    def test_process_environment_can_override_file(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / ".env"
            path.write_text("BITQUERY_TOKEN=file\n", encoding="utf-8")
            path.chmod(0o600)
            with patch.dict(os.environ, {"BITQUERY_TOKEN": "process"}):
                self.assertEqual("process", load_credentials(path).bitquery_token)

    def test_allows_pons_public_poll_configuration(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / ".env"
            path.write_text(
                "PONS_PUBLIC_POLL_RPC_URL=https://rpc.example\n"
                "PONS_PUBLIC_POLL_PRIMARY=1\n",
                encoding="utf-8",
            )
            path.chmod(0o600)
            values = load_env_file(path)
            self.assertEqual("1", values["PONS_PUBLIC_POLL_PRIMARY"])


if __name__ == "__main__":
    unittest.main()
