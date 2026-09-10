from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


ALLOWED_KEYS = {
    "MEME_RADAR_DB_PATH",
    "MEME_RADAR_HEALTH_PATH",
    "MEME_RADAR_FOMO_POLL_SECONDS",
    "MEME_RADAR_FOMO_ENABLED",
    "MEME_RADAR_CLANKER_POLL_SECONDS",
    "MEME_RADAR_ENRICH_MAX_PER_HOUR",
    "MEME_RADAR_BACKFILL_MAX_MINUTES",
    "MEME_RADAR_BITQUERY_ENABLED",
    "MEME_RADAR_SAMPLE_ENABLED",
    "MEME_RADAR_SAMPLE_MAX_PER_HOUR",
    "MEME_RADAR_TELEGRAM_ENABLED",
    "MEME_RADAR_TELEGRAM_MIN_INTERVAL_SECONDS",
    "MEME_RADAR_TELEGRAM_MAX_ATTEMPTS",
    "MEME_RADAR_TELEGRAM_MAX_PER_HOUR",
    "MEME_RADAR_NATIVE_RPC_MODE",
    "MEME_RADAR_NATIVE_RPC_BACKFILL_BLOCKS",
    "MEME_RADAR_NATIVE_RPC_PRIMARY_PROBE_SECONDS",
    "MEME_RADAR_NATIVE_RPC_PRIMARY_RECOVERY_SUCCESSES",
    "MEME_RADAR_GMGN_ENABLED",
    "MEME_RADAR_GMGN_MAX_PER_HOUR",
    "MEME_RADAR_GMGN_CACHE_SECONDS",
    "MEME_RADAR_GMGN_DISCOVERY_ENABLED",
    "MEME_RADAR_GMGN_DISCOVERY_POLL_SECONDS",
    "MEME_RADAR_GMGN_DISCOVERY_MAX_PER_HOUR",
    "BITQUERY_TOKEN",
    "BITQUERY_TOKEN_STANDBY",
    "FOMO_API_KEY",
    "FOMO_API_KEY_STANDBY",
    "GOPLUS_APP_KEY",
    "GOPLUS_APP_SECRET",
    "GOPLUS_STANDBY_APP_KEY",
    "GOPLUS_STANDBY_APP_SECRET",
    "GOPLUS_STANDBY_ENABLED",
    "ETHERSCAN_API_KEY",
    "BSC_RPC_URL",
    "BSC_WSS_URL",
    "BSC_RPC_URL_STANDBY",
    "BSC_WSS_URL_STANDBY",
    "BASE_RPC_URL",
    "BASE_WSS_URL",
    "BASE_RPC_URL_STANDBY",
    "BASE_WSS_URL_STANDBY",
    "ROBINHOOD_RPC_URL",
    "ROBINHOOD_WSS_URL",
    "ROBINHOOD_RPC_URL_STANDBY",
    "ROBINHOOD_WSS_URL_STANDBY",
    "PONS_PUBLIC_POLL_RPC_URL",
    "PONS_PUBLIC_POLL_PRIMARY",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "TELEGRAM_TOPIC_ID",
    "X_BEARER_TOKEN",
}


@dataclass(frozen=True)
class CredentialConfig:
    bitquery_token: str
    bitquery_token_standby: str
    fomo_api_key: str
    fomo_api_key_standby: str
    goplus_app_key: str
    goplus_app_secret: str
    goplus_standby_app_key: str
    goplus_standby_app_secret: str
    goplus_standby_enabled: bool
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_topic_id: str = ""
    bsc_rpc_url: str = ""
    bsc_wss_url: str = ""
    bsc_rpc_url_standby: str = ""
    bsc_wss_url_standby: str = ""
    base_rpc_url: str = ""
    base_wss_url: str = ""
    base_rpc_url_standby: str = ""
    base_wss_url_standby: str = ""
    robinhood_rpc_url: str = ""
    robinhood_wss_url: str = ""
    robinhood_rpc_url_standby: str = ""
    robinhood_wss_url_standby: str = ""


def _parse_line(line: str, number: int) -> Optional[tuple]:
    line = line.rstrip("\n")
    if line.endswith("\r"):
        line = line[:-1]
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if "=" not in line:
        raise ValueError("invalid env line %d" % number)
    key, value = line.split("=", 1)
    key = key.strip()
    if key not in ALLOWED_KEYS:
        raise ValueError("unknown env key %s" % key)
    if "\x00" in value or "\r" in value or "\n" in value:
        raise ValueError("invalid env value")
    return key, value.strip()


def load_env_file(path: Path) -> Dict[str, str]:
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("env path must be a regular file")
    if info.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise PermissionError("env file must be mode 0600")
    result: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            parsed = _parse_line(line, number)
            if parsed is None:
                continue
            key, value = parsed
            if key in result:
                raise ValueError("duplicate env key %s" % key)
            result[key] = value
    return result


def load_credentials(path: Path) -> CredentialConfig:
    file_values = load_env_file(path)

    def value(key: str) -> str:
        return os.environ.get(key, file_values.get(key, "")).strip()

    enabled = value("GOPLUS_STANDBY_ENABLED")
    if enabled not in {"", "0", "1"}:
        raise ValueError("invalid GOPLUS_STANDBY_ENABLED")
    return CredentialConfig(
        bitquery_token=value("BITQUERY_TOKEN"),
        bitquery_token_standby=value("BITQUERY_TOKEN_STANDBY"),
        fomo_api_key=value("FOMO_API_KEY"),
        fomo_api_key_standby=value("FOMO_API_KEY_STANDBY"),
        goplus_app_key=value("GOPLUS_APP_KEY"),
        goplus_app_secret=value("GOPLUS_APP_SECRET"),
        goplus_standby_app_key=value("GOPLUS_STANDBY_APP_KEY"),
        goplus_standby_app_secret=value("GOPLUS_STANDBY_APP_SECRET"),
        goplus_standby_enabled=enabled == "1",
        telegram_bot_token=value("TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=value("TELEGRAM_CHAT_ID"),
        telegram_topic_id=value("TELEGRAM_TOPIC_ID"),
        bsc_rpc_url=value("BSC_RPC_URL"),
        bsc_wss_url=value("BSC_WSS_URL"),
        bsc_rpc_url_standby=value("BSC_RPC_URL_STANDBY"),
        bsc_wss_url_standby=value("BSC_WSS_URL_STANDBY"),
        base_rpc_url=value("BASE_RPC_URL"),
        base_wss_url=value("BASE_WSS_URL"),
        base_rpc_url_standby=value("BASE_RPC_URL_STANDBY"),
        base_wss_url_standby=value("BASE_WSS_URL_STANDBY"),
        robinhood_rpc_url=value("ROBINHOOD_RPC_URL"),
        robinhood_wss_url=value("ROBINHOOD_WSS_URL"),
        robinhood_rpc_url_standby=value("ROBINHOOD_RPC_URL_STANDBY"),
        robinhood_wss_url_standby=value("ROBINHOOD_WSS_URL_STANDBY"),
    )
