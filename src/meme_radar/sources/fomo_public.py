from __future__ import annotations

from typing import Dict, Optional

from ..adapters import parse_fomo_alerts
from ..config import RadarConfig
from ..http_json import JsonTransport
from ..models import ParseBatch


class FomoPublicSource:
    BASE_URL = "https://api.fomoapi.io"

    def __init__(
        self,
        transport: JsonTransport,
        *,
        api_key: Optional[str] = None,
        config: RadarConfig = RadarConfig(),
    ) -> None:
        self._transport = transport
        self._api_key = api_key
        self._config = config

    def _headers(self) -> Dict[str, str]:
        if not self._api_key:
            return {}
        if (
            len(self._api_key) > 4000
            or "\r" in self._api_key
            or "\n" in self._api_key
        ):
            raise ValueError("invalid FOMO API key")
        return {"Authorization": "Bearer " + self._api_key}

    def fetch_alerts(self, limit: int = 20) -> ParseBatch:
        _, batch = self.fetch_alerts_with_result(limit)
        return batch

    def fetch_alerts_with_result(self, limit: int = 20):
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        result = self._transport.get_json(
            self.BASE_URL + "/v2/alerts?limit=%d" % limit,
            self._headers(),
        )
        if not isinstance(result.data, dict):
            raise ValueError("FOMO alerts response must be an object")
        batch = parse_fomo_alerts(
            result.data,
            observed_at_ms=result.received_at_ms,
            received_at_ms=result.received_at_ms,
            config=self._config,
        )
        return result, batch

    def fetch_token_board(self, board: str, limit: int = 20):
        if not self._api_key:
            raise RuntimeError("FOMO API key required for token boards")
        if board not in {"trending", "most-held", "graduated"}:
            raise ValueError("unknown token board")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        result = self._transport.get_json(
            self.BASE_URL
            + "/v2/leaderboard/tokens/%s?limit=%d" % (board, limit),
            self._headers(),
        )
        if not isinstance(result.data, dict):
            raise ValueError("FOMO board response must be an object")
        return result
