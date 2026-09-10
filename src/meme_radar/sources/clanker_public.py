from __future__ import annotations

import time
from typing import Dict
from urllib.parse import urlencode

from ..adapters import parse_clanker_tokens
from ..config import RadarConfig
from ..http_json import JsonTransport
from ..launchpads import (
    CLANKER_V4_BASE_FACTORY,
    CLANKER_V4_TOKEN_CREATED_TOPIC,
)
from ..models import ParseBatch
from ..normalize import normalize_address


class ClankerPublicSource:
    BASE_URL = "https://www.clanker.world"

    def __init__(
        self,
        transport: JsonTransport,
        *,
        config: RadarConfig = RadarConfig(),
        now=time.time,
        lookback_seconds: int = 600,
    ) -> None:
        if not 60 <= lookback_seconds <= 3600:
            raise ValueError("invalid Clanker lookback")
        self._transport = transport
        self._config = config
        self._now = now
        self._lookback_seconds = lookback_seconds

    def _load_base_factory(self):
        result = self._transport.get_json(
            self.BASE_URL + "/api/metadata/factories"
        )
        if not isinstance(result.data, list):
            raise ValueError("Clanker factory response must be a list")
        matches = [
            row for row in result.data
            if isinstance(row, dict) and row.get("name") == "clanker_v4_base"
        ]
        if len(matches) != 1:
            raise RuntimeError("Clanker Base factory identity drift")
        row: Dict = matches[0]
        address = normalize_address("base", row.get("address"))
        topic = str((row.get("events") or {}).get("tokenCreated", "")).lower()
        if (
            address != CLANKER_V4_BASE_FACTORY
            or topic != CLANKER_V4_TOKEN_CREATED_TOPIC
        ):
            raise RuntimeError("Clanker Base factory metadata changed")
        return result, address

    def fetch_latest_base(self, limit: int = 20) -> ParseBatch:
        _, _, batch = self.fetch_latest_base_with_results(limit)
        return batch

    def fetch_latest_base_with_results(self, limit: int = 20):
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 20:
            raise ValueError("limit must be 1..20")
        metadata_result, factory = self._load_base_factory()
        query = urlencode(
            {
                "chainId": 8453,
                "startDate": int(self._now()) - self._lookback_seconds,
                "limit": limit,
                "sort": "desc",
            }
        )
        result = self._transport.get_json(
            self.BASE_URL + "/api/tokens?" + query
        )
        if not isinstance(result.data, dict):
            raise ValueError("Clanker token response must be an object")
        batch = parse_clanker_tokens(
            result.data,
            expected_chain="base",
            allowed_factories=(factory,),
            observed_at_ms=result.received_at_ms,
            received_at_ms=result.received_at_ms,
            config=self._config,
        )
        return metadata_result, result, batch
