import unittest

from meme_radar.narrative import HotTerm, NarrativeTracker
from meme_radar.normalize import build_event


def event(index, chain="bsc", name="Bull Run", symbol="BULL"):
    address = "0x%040x" % (index + 1)
    when = 1_788_516_000_000 + index * 1000
    return build_event(
        source="fixture",
        source_event_id=str(index),
        event_kind="launch",
        chain=chain,
        token_address=address,
        source_published_at_ms=when,
        observed_at_ms=when,
        received_at_ms=when,
        token_created_at_ms=when,
        name=name,
        symbol=symbol,
    )


class NarrativeTests(unittest.TestCase):
    def test_burst_hot_term_and_cross_chain(self):
        tracker = NarrativeTracker(
            30 * 60 * 1000,
            [HotTerm("bull", "market", 1.0, "bsc")],
        )
        last = None
        for index in range(4):
            last = tracker.add(event(index))
        last = tracker.add(event(4, chain="base"))
        self.assertEqual(5, last.burst_count)
        self.assertEqual(2, last.cross_chain_count)
        self.assertAlmostEqual(0.7, last.hot_term_weight)

    def test_cluster_time_uses_event_clock_not_wall_clock(self):
        tracker = NarrativeTracker(1000)
        first = tracker.add(event(0))
        later_event = event(10)
        later = tracker.add(later_event)
        self.assertEqual(1, first.burst_count)
        self.assertEqual(1, later.burst_count)


if __name__ == "__main__":
    unittest.main()
