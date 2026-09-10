import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from meme_radar.adapters import parse_fourmeme
from meme_radar.models import FeatureSnapshot
from meme_radar.replay import replay, replay_digest
from meme_radar.storage import (
    claim_due_enrichment,
    claim_due_telegram,
    connect,
    enqueue_telegram,
    enqueue_observation_sample,
    finish_enrichment_job,
    initialize,
    mark_telegram_retry,
    mark_telegram_sent,
    reschedule_enrichment_job,
    schedule_enrichment_job,
    store_decision,
    store_event,
    store_provider_observation,
    get_runtime_state,
    set_runtime_state,
    store_raw_payload,
    telegram_outbox_counts,
)

from support import load_fixture


OBSERVED = 1_788_516_100_000
RECEIVED = OBSERVED + 50


class StorageReplayTests(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("fourmeme_events.json")
        self.batch = parse_fourmeme(
            self.payload,
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )

    def test_replay_is_order_independent_and_deduplicated(self):
        forward = replay(self.batch.events + (self.batch.events[0],))
        reverse = replay(tuple(reversed(self.batch.events)) + (self.batch.events[0],))
        self.assertEqual(2, len(forward))
        self.assertEqual(replay_digest(forward), replay_digest(reverse))

    def test_replay_rejects_conflicting_duplicate_identity(self):
        first = self.batch.events[0]
        conflict = replace(first, name="Different")
        with self.assertRaisesRegex(ValueError, "conflicting duplicate"):
            replay((first, conflict))

    def test_replay_accepts_same_event_observed_again(self):
        first = self.batch.events[0]
        repeated = replace(
            first,
            observed_at_ms=first.observed_at_ms + 1000,
            received_at_ms=first.received_at_ms + 1000,
            raw_sha256="f" * 64,
        )
        result = replay((repeated, first))
        self.assertEqual(1, len(result))

    def test_sqlite_v1_is_idempotent_and_has_foreign_keys(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            initialize(connection)
            initialize(connection)
            self.assertEqual(4, connection.execute("PRAGMA user_version").fetchone()[0])
            digest = store_raw_payload(
                connection,
                source="bitquery",
                batch_id="fixture-batch",
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
                is_backfill=False,
                payload=self.payload,
            )
            self.assertEqual(self.batch.raw_sha256, digest)
            self.assertTrue(store_event(connection, self.batch.events[0]))
            self.assertFalse(store_event(connection, self.batch.events[0]))
            repeated = replace(
                self.batch.events[0],
                observed_at_ms=self.batch.events[0].observed_at_ms + 1000,
                received_at_ms=self.batch.events[0].received_at_ms + 1000,
                raw_sha256=self.batch.events[0].raw_sha256,
            )
            self.assertFalse(store_event(connection, repeated))
            with self.assertRaisesRegex(RuntimeError, "conflicting event"):
                store_event(
                    connection,
                    replace(self.batch.events[0], name="Conflicting"),
                )
            decision = replay((self.batch.events[0],))[0]
            self.assertTrue(store_decision(connection, decision))
            self.assertFalse(store_decision(connection, decision))
            with self.assertRaisesRegex(RuntimeError, "conflicting decision"):
                store_decision(
                    connection,
                    replace(decision, opportunity_score=1),
                )
            counts = {
                table: connection.execute(
                    "SELECT COUNT(*) FROM %s" % table
                ).fetchone()[0]
                for table in (
                    "raw_payloads",
                    "raw_batches",
                    "radar_events",
                    "decisions",
                )
            }
            self.assertEqual(
                {"raw_payloads": 1, "raw_batches": 1, "radar_events": 1, "decisions": 1},
                counts,
            )
            self.assertEqual("ok", connection.execute("PRAGMA quick_check").fetchone()[0])
            self.assertTrue(
                store_provider_observation(
                    connection,
                    event_id=self.batch.events[0].event_id,
                    provider="fixture",
                    observed_at_ms=RECEIVED,
                    status="ok",
                    payload={"safe": True},
                    summary={"covered": True},
                )
            )
            self.assertFalse(
                store_provider_observation(
                    connection,
                    event_id=self.batch.events[0].event_id,
                    provider="fixture",
                    observed_at_ms=RECEIVED,
                    status="ok",
                    payload={"safe": True},
                    summary={"covered": True},
                )
            )
            with self.assertRaises(sqlite3.IntegrityError):
                store_event(
                    connection,
                    replace(
                        self.batch.events[1],
                        raw_sha256="f" * 64,
                    ),
                )
            connection.close()

    def test_new_database_is_mode_0600(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            connection = connect(path)
            connection.close()
            self.assertEqual(0o600, path.stat().st_mode & 0o777)

    def test_sqlite_uses_wal_and_busy_timeout(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            self.assertEqual(
                "wal",
                connection.execute("PRAGMA journal_mode").fetchone()[0],
            )
            self.assertEqual(
                10_000,
                connection.execute("PRAGMA busy_timeout").fetchone()[0],
            )
            self.assertEqual(
                2,
                connection.execute("PRAGMA synchronous").fetchone()[0],
            )
            connection.close()

    def test_wal_writer_is_not_blocked_by_reader(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            writer = connect(path)
            initialize(writer)
            reader = sqlite3.connect(str(path))
            reader.execute("BEGIN")
            reader.execute("SELECT COUNT(*) FROM radar_events").fetchone()
            writer.execute(
                "INSERT INTO runtime_state (key,value_json,updated_at_ms) "
                "VALUES ('wal-test','true',1)"
            )
            writer.commit()
            self.assertEqual(
                1,
                writer.execute(
                    "SELECT COUNT(*) FROM runtime_state WHERE key='wal-test'"
                ).fetchone()[0],
            )
            reader.close()
            writer.close()

    def test_schema_v1_upgrades_with_enrichment_extension(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            initialize(connection)
            connection.execute("PRAGMA user_version = 1")
            initialize(connection)
            self.assertEqual(4, connection.execute("PRAGMA user_version").fetchone()[0])
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("provider_observations", tables)
            self.assertIn("runtime_state", tables)
            self.assertIn("telegram_delivery_claims", tables)
            self.assertIn("enrichment_jobs", tables)
            indexes = {
                row[1]
                for row in connection.execute("PRAGMA index_list(radar_events)")
            }
            self.assertIn("idx_events_seed_reconcile", indexes)
            set_runtime_state(connection, "checkpoint", {"block": 1}, RECEIVED)
            self.assertEqual(
                {"block": 1},
                get_runtime_state(connection, "checkpoint"),
            )
            connection.close()

    def test_enrichment_jobs_are_priority_ordered_and_recoverable(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            initialize(connection)
            store_raw_payload(
                connection,
                source="bitquery",
                batch_id="fixture",
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
                is_backfill=False,
                payload=self.payload,
            )
            for event in self.batch.events:
                store_event(connection, event)
            for event, score in zip(self.batch.events, (40, 80)):
                schedule_enrichment_job(
                    connection,
                    event=event,
                    features=FeatureSnapshot(
                        event_id=event.event_id,
                        evaluated_at_ms=RECEIVED,
                        metadata_complete=True,
                    ),
                    priority_version="priority_v1",
                    priority_score=score,
                    priority_reasons=("TEST",),
                    due_at_ms=RECEIVED + 1000,
                    expires_at_ms=RECEIVED + 600_000,
                )
            with self.assertRaisesRegex(RuntimeError, "conflicting enrichment"):
                schedule_enrichment_job(
                    connection,
                    event=self.batch.events[0],
                    features=FeatureSnapshot(
                        event_id=self.batch.events[0].event_id,
                        evaluated_at_ms=RECEIVED,
                        metadata_complete=True,
                    ),
                    priority_version="priority_v1",
                    priority_score=41,
                    priority_reasons=("TEST",),
                    due_at_ms=RECEIVED + 1000,
                    expires_at_ms=RECEIVED + 600_000,
                )
            self.assertIsNone(
                claim_due_enrichment(
                    connection,
                    now_ms=RECEIVED + 1000,
                    initial_available=False,
                )
            )
            claimed = claim_due_enrichment(
                connection,
                now_ms=RECEIVED + 1000,
            )
            self.assertEqual(self.batch.events[1].event_id, claimed["event_id"])
            reschedule_enrichment_job(
                connection,
                claimed,
                stage="dex_recheck",
                dex_attempts=1,
                security_attempts=0,
                due_at_ms=RECEIVED + 2000,
                now_ms=RECEIVED + 1001,
                last_error="NOT_COVERED",
            )
            claimed = claim_due_enrichment(
                connection,
                now_ms=RECEIVED + 2000,
            )
            finish_enrichment_job(
                connection,
                claimed,
                result_code="DEX_UNAVAILABLE",
                now_ms=RECEIVED + 2001,
            )
            processing = claim_due_enrichment(
                connection,
                now_ms=RECEIVED + 2001,
            )
            self.assertEqual(self.batch.events[0].event_id, processing["event_id"])
            initialize(connection)
            state = connection.execute(
                "SELECT state, last_error FROM enrichment_jobs "
                "WHERE event_id=?",
                (processing["event_id"],),
            ).fetchone()
            self.assertEqual(("pending", "RECOVERED_AFTER_RESTART"), tuple(state))
            connection.close()

    def test_telegram_outbox_is_deduplicated_and_retryable(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            initialize(connection)
            store_raw_payload(
                connection,
                source="bitquery",
                batch_id="fixture",
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
                is_backfill=False,
                payload=self.payload,
            )
            event = self.batch.events[0]
            store_event(connection, event)
            decision = replace(
                replay((event,))[0],
                evaluated_at_ms=RECEIVED + 1,
                risk_verdict="pass",
                confidence_score=90,
                opportunity_score=80,
                delivery="strong",
            )
            store_decision(connection, decision)
            self.assertTrue(
                enqueue_telegram(
                    connection,
                    decision=decision,
                    dedupe_key="radar_v1:bsc:token",
                    message_text="test",
                    now_ms=RECEIVED + 2,
                )
            )
            self.assertFalse(
                enqueue_telegram(
                    connection,
                    decision=decision,
                    dedupe_key="radar_v1:bsc:token",
                    message_text="test",
                    now_ms=RECEIVED + 3,
                )
            )
            item = claim_due_telegram(
                connection,
                now_ms=RECEIVED + 2,
                max_attempts=5,
            )
            self.assertEqual(1, item["attempts"])
            mark_telegram_retry(
                connection,
                item,
                next_attempt_at_ms=RECEIVED + 10_000,
                error_code="NETWORK_ERROR",
            )
            self.assertIsNone(
                claim_due_telegram(
                    connection,
                    now_ms=RECEIVED + 9_999,
                    max_attempts=5,
                )
            )
            item = claim_due_telegram(
                connection,
                now_ms=RECEIVED + 10_000,
                max_attempts=5,
            )
            mark_telegram_sent(connection, item, sent_at_ms=RECEIVED + 10_001)
            self.assertEqual(
                {"pending": 0, "sent": 1},
                telegram_outbox_counts(connection),
            )
            connection.close()

    def test_observation_samples_have_a_persistent_hourly_cap(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            initialize(connection)
            store_raw_payload(
                connection,
                source="bitquery",
                batch_id="sample-fixture",
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
                is_backfill=False,
                payload=self.payload,
            )
            statuses = []
            for index in range(4):
                event = replace(
                    self.batch.events[0],
                    event_id="sample-event-%d" % index,
                    source_event_id="sample-source-%d" % index,
                    token_address="0x" + str(index + 1) * 40,
                )
                store_event(connection, event)
                decision = replace(
                    replay((event,))[0],
                    evaluated_at_ms=RECEIVED + index,
                    risk_verdict="review",
                    confidence_score=50,
                    opportunity_score=35,
                    delivery="weak",
                )
                store_decision(connection, decision)
                statuses.append(
                    enqueue_observation_sample(
                        connection,
                        decision=decision,
                        dedupe_key="sample_v1:bsc:%d" % index,
                        message_text="sample %d" % index,
                        now_ms=RECEIVED + index,
                        max_per_hour=3,
                    )
                )
            self.assertEqual(
                ["enqueued", "enqueued", "enqueued", "rate_limited"],
                statuses,
            )
            self.assertEqual(
                3,
                connection.execute(
                    "SELECT COUNT(*) FROM telegram_outbox"
                ).fetchone()[0],
            )
            connection.close()

    def test_sqlite_rejects_conflicting_batch_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect(Path(temp) / "radar.db")
            initialize(connection)
            store_raw_payload(
                connection,
                source="bitquery",
                batch_id="same",
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
                is_backfill=False,
                payload=self.payload,
            )
            with self.assertRaisesRegex(RuntimeError, "conflicting raw batch"):
                store_raw_payload(
                    connection,
                    source="bitquery",
                    batch_id="same",
                    observed_at_ms=OBSERVED + 1,
                    received_at_ms=RECEIVED + 1,
                    is_backfill=False,
                    payload={"different": True},
                )
            connection.close()


if __name__ == "__main__":
    unittest.main()
