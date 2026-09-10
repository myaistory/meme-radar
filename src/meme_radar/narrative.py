from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set, Tuple

from .models import RadarEvent


_CJK = re.compile(r"[\u4e00-\u9fff]")
_STOCK_QUOTES = {
    "AMC",
    "GME",
    "HOOD",
    "TSLA",
    "NVDA",
    "SPY",
    "AAPL",
    "MSTR",
    "COIN",
    "PLTR",
}


def normalize_term(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "").lower()
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", text)


def cluster_key(event: RadarEvent) -> str:
    symbol = normalize_term(event.symbol)
    name = normalize_term(event.name)
    if symbol and len(symbol) <= 10:
        return symbol
    if _CJK.search(name):
        return name[:4]
    return name[:12]


@dataclass(frozen=True)
class HotTerm:
    term: str
    bucket: str
    weight: float
    preferred_chain: str


@dataclass(frozen=True)
class NarrativeFeatures:
    key: str
    burst_count: int
    cross_chain_count: int
    hot_term_weight: float
    quote_narrative: bool


class NarrativeTracker:
    def __init__(
        self,
        window_ms: int,
        hot_terms: Iterable[HotTerm] = (),
    ) -> None:
        if window_ms <= 0:
            raise ValueError("window must be positive")
        self._window_ms = window_ms
        self._hot_terms = tuple(
            HotTerm(
                normalize_term(item.term),
                item.bucket,
                item.weight,
                item.preferred_chain,
            )
            for item in hot_terms
            if normalize_term(item.term)
        )
        self._clusters: Dict[str, List[Tuple[int, RadarEvent]]] = {}

    def add(self, event: RadarEvent) -> NarrativeFeatures:
        key = cluster_key(event)
        if not key:
            key = "address:" + event.token_address
        cutoff = event.observed_at_ms - self._window_ms
        rows = [
            item for item in self._clusters.get(key, [])
            if item[0] >= cutoff
        ]
        rows.append((event.observed_at_ms, event))
        rows.sort(key=lambda item: (item[0], item[1].event_id))
        self._clusters[key] = rows

        chains: Set[str] = {item.chain for _, item in rows}
        matched_weight = 0.0
        for hot in self._hot_terms:
            if hot.term in key:
                multiplier = 1.3 if hot.preferred_chain == event.chain else 0.7
                matched_weight = max(matched_weight, hot.weight * multiplier)
        return NarrativeFeatures(
            key=key,
            burst_count=len(rows),
            cross_chain_count=len(chains),
            hot_term_weight=matched_weight,
            quote_narrative=event.quote_symbol.upper() in _STOCK_QUOTES,
        )
