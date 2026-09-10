from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Tuple

from .models import FeatureSnapshot, RadarEvent


@dataclass(frozen=True)
class PriorityDecision:
    score: int
    reason_codes: Tuple[str, ...]


class PriorityTracker:
    """Ranks API spend; it never changes risk or delivery decisions."""

    def __init__(self, creator_window_ms: int = 10 * 60 * 1000) -> None:
        if creator_window_ms <= 0:
            raise ValueError("creator window must be positive")
        self._creator_window_ms = creator_window_ms
        self._creator_times: Dict[str, Deque[int]] = defaultdict(deque)

    def score(
        self,
        event: RadarEvent,
        features: FeatureSnapshot,
    ) -> PriorityDecision:
        if event.event_id != features.event_id:
            raise ValueError("event and features do not match")
        reasons = []
        score = self._freshness(event, reasons)
        if features.metadata_complete:
            score += 10
            reasons.append("METADATA_COMPLETE_10")
        if event.creator:
            score += 5
            reasons.append("CREATOR_PRESENT_5")
        score += self._narrative(features, reasons)
        chain_bonus = {"base": 10, "bsc": 4, "robinhood": 4}.get(
            event.chain,
            0,
        )
        score += chain_bonus
        reasons.append("CHAIN_FAIRNESS_%d" % chain_bonus)
        score += self._creator_penalty(event, reasons)
        return PriorityDecision(max(0, min(100, score)), tuple(reasons))

    @staticmethod
    def _freshness(event: RadarEvent, reasons: list) -> int:
        age = event.observed_at_ms - event.source_published_at_ms
        for ceiling, points in ((5000, 30), (15000, 25), (60000, 15)):
            if age <= ceiling:
                reasons.append("SOURCE_FRESH_%d" % points)
                return points
        reasons.append("SOURCE_FRESH_5")
        return 5

    @staticmethod
    def _narrative(features: FeatureSnapshot, reasons: list) -> int:
        burst = features.narrative_burst
        if burst >= 5:
            points = 20
        elif burst >= 3:
            points = 12
        elif burst >= 2:
            points = 5
        else:
            points = 0
        if points:
            reasons.append("NARRATIVE_BURST_%d" % points)
        if features.cross_chain_count >= 2:
            points += 10
            reasons.append("CROSS_CHAIN_10")
        return points

    def _creator_penalty(self, event: RadarEvent, reasons: list) -> int:
        if not event.creator:
            return 0
        rows = self._creator_times[event.chain + ":" + event.creator]
        cutoff = event.observed_at_ms - self._creator_window_ms
        while rows and rows[0] < cutoff:
            rows.popleft()
        rows.append(event.observed_at_ms)
        count = len(rows)
        if count >= 10:
            penalty = -20
        elif count >= 5:
            penalty = -10
        elif count >= 3:
            penalty = -5
        else:
            penalty = 0
        if penalty:
            reasons.append("CREATOR_BURST_%d" % penalty)
        return penalty
