from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from ..http_json import JsonTransport
from ..normalize import normalize_address, normalize_chain


DEX_CHAIN_IDS = {
    "ethereum": "ethereum",
    "bsc": "bsc",
    "base": "base",
    "robinhood": "robinhood",
    "solana": "solana",
}


@dataclass(frozen=True)
class MarketSnapshot:
    chain: str
    token_address: str
    observed_at_ms: int
    pair_count: int
    best_pair_address: str
    pair_created_at_ms: Optional[int]
    price_usd: Optional[float]
    liquidity_usd: Optional[float]
    volume_5m_usd: Optional[float]
    volume_1h_usd: Optional[float]
    volume_24h_usd: Optional[float]
    buy_transactions_5m: Optional[int]
    sell_transactions_5m: Optional[int]
    buy_transactions_1h: Optional[int]
    sell_transactions_1h: Optional[int]
    buy_transactions_24h: Optional[int]
    sell_transactions_24h: Optional[int]
    bytes_read: int
    elapsed_ms: int
    market_cap_usd: Optional[float] = None
    fdv_usd: Optional[float] = None
    creator_inbound_count: Optional[int] = None
    creator_outbound_count: Optional[int] = None
    creator_retention_ratio: Optional[float] = None
    identity_count: Optional[int] = None
    identity_market_cap_rank: Optional[int] = None
    external_buy_transactions_5m: Optional[int] = None
    holder_count: Optional[int] = None


def _number(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result < 0 or result != result or result in (float("inf"), float("-inf")):
        return None
    return result


def _count(value: Any) -> Optional[int]:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


class DexScreenerSource:
    BASE_URL = "https://api.dexscreener.com"

    def __init__(self, transport: JsonTransport) -> None:
        self._transport = transport

    def token_market(self, chain: str, token_address: str) -> MarketSnapshot:
        normalized_chain = normalize_chain(chain)
        address = normalize_address(normalized_chain, token_address)
        response = self._transport.get_json(
            self.BASE_URL + "/latest/dex/tokens/" + quote(address, safe="")
        )
        data = response.data
        if not isinstance(data, dict) or "pairs" not in data:
            raise RuntimeError("DexScreener pairs missing")
        pairs = data.get("pairs")
        if pairs is None:
            pairs = []
        if not isinstance(pairs, list):
            raise RuntimeError("DexScreener pairs invalid")
        rows: List[Dict[str, Any]] = []
        for item in pairs[:400]:
            if not isinstance(item, dict):
                continue
            if str(item.get("chainId", "")).lower() != DEX_CHAIN_IDS[normalized_chain]:
                continue
            related = set()
            for side in ("baseToken", "quoteToken"):
                token = item.get(side)
                value = token.get("address") if isinstance(token, dict) else None
                if value:
                    try:
                        related.add(normalize_address(normalized_chain, value))
                    except ValueError:
                        continue
            if address not in related:
                continue
            rows.append(item)
        rows.sort(
            key=lambda item: _number((item.get("liquidity") or {}).get("usd")) or 0,
            reverse=True,
        )
        best = rows[0] if rows else {}
        return MarketSnapshot(
            chain=normalized_chain,
            token_address=address,
            observed_at_ms=response.received_at_ms,
            pair_count=len(rows),
            best_pair_address=str(best.get("pairAddress", ""))[:128],
            pair_created_at_ms=_count(best.get("pairCreatedAt")),
            price_usd=_number(best.get("priceUsd")),
            liquidity_usd=_number((best.get("liquidity") or {}).get("usd")),
            volume_5m_usd=_number((best.get("volume") or {}).get("m5")),
            volume_1h_usd=_number((best.get("volume") or {}).get("h1")),
            volume_24h_usd=_number((best.get("volume") or {}).get("h24")),
            buy_transactions_5m=_count(
                ((best.get("txns") or {}).get("m5") or {}).get("buys")
            ),
            sell_transactions_5m=_count(
                ((best.get("txns") or {}).get("m5") or {}).get("sells")
            ),
            buy_transactions_1h=_count(
                ((best.get("txns") or {}).get("h1") or {}).get("buys")
            ),
            sell_transactions_1h=_count(
                ((best.get("txns") or {}).get("h1") or {}).get("sells")
            ),
            buy_transactions_24h=_count(
                ((best.get("txns") or {}).get("h24") or {}).get("buys")
            ),
            sell_transactions_24h=_count(
                ((best.get("txns") or {}).get("h24") or {}).get("sells")
            ),
            bytes_read=response.bytes_read,
            elapsed_ms=response.elapsed_ms,
            market_cap_usd=_number(best.get("marketCap")),
            fdv_usd=_number(best.get("fdv")),
        )
