import unittest

from meme_radar.models import FeatureSnapshot, RadarEvent
from meme_radar.priority import PriorityTracker


NOW = 1_788_600_000_000


def event(*, chain="bsc", creator="0x" + "2" * 40, delay_ms=1000):
    return RadarEvent(
        schema_version=1,
        event_id="event-" + chain + "-" + str(delay_ms),
        source="native_rpc",
        source_event_id="source-" + chain + "-" + str(delay_ms),
        event_kind="launch",
        chain=chain,
        chain_id={"bsc": 56, "base": 8453, "robinhood": 4663}[chain],
        token_address="0x" + "1" * 40,
        source_published_at_ms=NOW,
        observed_at_ms=NOW + delay_ms,
        received_at_ms=NOW + delay_ms,
        token_created_at_ms=NOW,
        name="Meme",
        symbol="MEME",
        creator=creator,
    )


class PriorityTests(unittest.TestCase):
    def test_fresh_cross_chain_burst_ranks_above_plain_event(self):
        plain_tracker = PriorityTracker()
        rich_tracker = PriorityTracker()
        plain_event = event()
        plain = plain_tracker.score(
            plain_event,
            FeatureSnapshot(
                event_id=plain_event.event_id,
                evaluated_at_ms=plain_event.received_at_ms,
                metadata_complete=True,
            ),
        )
        rich_event = event(chain="base")
        rich = rich_tracker.score(
            rich_event,
            FeatureSnapshot(
                event_id=rich_event.event_id,
                evaluated_at_ms=rich_event.received_at_ms,
                metadata_complete=True,
                narrative_burst=5,
                cross_chain_count=2,
            ),
        )
        self.assertGreater(rich.score, plain.score)
        self.assertIn("CROSS_CHAIN_10", rich.reason_codes)

    def test_creator_burst_is_only_a_priority_penalty(self):
        tracker = PriorityTracker()
        decisions = []
        for index in range(5):
            current = event(delay_ms=1000 + index)
            decisions.append(
                tracker.score(
                    current,
                    FeatureSnapshot(
                        event_id=current.event_id,
                        evaluated_at_ms=current.received_at_ms,
                        metadata_complete=True,
                    ),
                )
            )
        self.assertLess(decisions[-1].score, decisions[0].score)
        self.assertIn("CREATOR_BURST_-10", decisions[-1].reason_codes)


if __name__ == "__main__":
    unittest.main()
