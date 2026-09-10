from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Set, Tuple

from .config import RadarConfig
from .models import Decision, FeatureSnapshot, RadarEvent
from .narrative import HotTerm, NarrativeTracker
from .normalize import canonical_json
from .scoring import evaluate


def replay(
    events: Iterable[RadarEvent],
    *,
    hot_terms: Sequence[HotTerm] = (),
    config: RadarConfig = RadarConfig(),
) -> Tuple[Decision, ...]:
    ordered = sorted(
        events,
        key=lambda item: (
            item.observed_at_ms,
            item.source_published_at_ms,
            item.source,
            item.source_event_id,
            item.event_id,
        ),
    )
    tracker = NarrativeTracker(config.narrative_window_ms, hot_terms)
    seen: Dict[Tuple[str, str], RadarEvent] = {}
    token_sources: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    decisions: List[Decision] = []

    for event in ordered:
        source_key = (event.source, event.source_event_id)
        previous = seen.get(source_key)
        if previous is not None:
            if previous.stable_dict() != event.stable_dict():
                raise ValueError("conflicting duplicate source event")
            continue
        seen[source_key] = event
        token_key = (event.chain, event.token_address)
        token_sources[token_key].add(event.source)
        narrative = tracker.add(event)
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=event.received_at_ms,
            source_count=len(token_sources[token_key]),
            metadata_complete=bool(event.name and event.symbol),
            narrative_burst=narrative.burst_count,
            cross_chain_count=narrative.cross_chain_count,
            hot_term_weight=narrative.hot_term_weight,
            quote_narrative=narrative.quote_narrative,
        )
        decisions.append(evaluate(event, features, config))
    return tuple(decisions)


def replay_digest(decisions: Iterable[Decision]) -> str:
    payload = [item.to_dict() for item in decisions]
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
