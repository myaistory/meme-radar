from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import urlencode

from ..http_json import HttpJsonResult, JsonTransport
from ..models import KNOWN_CHAIN_IDS
from ..normalize import normalize_address, normalize_chain


@dataclass(frozen=True)
class GoPlusSecurityResult:
    chain: str
    token_address: str
    observed_at_ms: int
    payload: Dict[str, Any]
    bytes_read: int
    elapsed_ms: int


class ProviderUnavailable(RuntimeError):
    pass


class ProviderApiError(RuntimeError):
    def __init__(self, api_code: Any) -> None:
        super().__init__("provider api error")
        self.api_code = api_code


class GoPlusSource:
    BASE_URL = "https://api.gopluslabs.io"

    def __init__(
        self,
        transport: JsonTransport,
        *,
        app_key: str,
        app_secret: str,
        now=time.time,
    ) -> None:
        if not app_key or not app_secret:
            raise ValueError("GoPlus credentials required")
        self._transport = transport
        self._app_key = app_key
        self._app_secret = app_secret
        self._now = now
        self._authorization = ""
        self._expires_at = 0.0

    def _authorization_header(self) -> str:
        now = self._now()
        if self._authorization and now < self._expires_at - 60:
            return self._authorization
        timestamp = int(now)
        sign = hashlib.sha1(
            (
                self._app_key
                + str(timestamp)
                + self._app_secret
            ).encode("utf-8")
        ).hexdigest()
        response = self._transport.post_json(
            self.BASE_URL + "/api/v1/token",
            {
                "app_key": self._app_key,
                "sign": sign,
                "time": timestamp,
            },
        )
        data = response.data
        if not isinstance(data, dict):
            raise RuntimeError("GoPlus access token response invalid")
        if data.get("code") != 1:
            raise ProviderApiError(data.get("code"))
        result = data.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("GoPlus access token missing")
        token = result.get("access_token")
        expires_in = result.get("expires_in")
        if not isinstance(token, str) or not token:
            raise RuntimeError("GoPlus access token missing")
        if token.startswith("Bearer "):
            authorization = token
        elif token.count(".") == 2:
            authorization = "Bearer " + token
        else:
            raise RuntimeError("GoPlus access token shape invalid")
        try:
            ttl = int(expires_in)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("GoPlus token expiry invalid") from exc
        if ttl <= 120:
            raise RuntimeError("GoPlus token expiry too short")
        self._authorization = authorization
        self._expires_at = now + ttl
        return authorization

    def token_security(
        self,
        chain: str,
        token_address: str,
    ) -> GoPlusSecurityResult:
        normalized_chain = normalize_chain(chain)
        if normalized_chain not in {"ethereum", "bsc", "base", "robinhood"}:
            raise ValueError("GoPlus EVM token security chain required")
        address = normalize_address(normalized_chain, token_address)
        chain_id = KNOWN_CHAIN_IDS[normalized_chain]
        url = (
            self.BASE_URL
            + "/api/v1/token_security/%d?" % chain_id
            + urlencode({"contract_addresses": address})
        )
        response: HttpJsonResult = self._transport.get_json(
            url,
            {"Authorization": self._authorization_header()},
        )
        data = response.data
        if not isinstance(data, dict):
            raise RuntimeError("GoPlus token security response invalid")
        if data.get("code") != 1:
            raise ProviderApiError(data.get("code"))
        result = data.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("GoPlus token security missing result")
        lowered = {str(key).lower(): value for key, value in result.items()}
        payload = lowered.get(address.lower())
        if not isinstance(payload, dict):
            raise ProviderUnavailable("GoPlus token not covered")
        return GoPlusSecurityResult(
            chain=normalized_chain,
            token_address=address,
            observed_at_ms=response.received_at_ms,
            payload=payload,
            bytes_read=response.bytes_read,
            elapsed_ms=response.elapsed_ms,
        )
