from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Dict, Optional

from .adapters import parse_fourmeme, parse_pons_launched
from .bitquery_history import BitqueryHistorySource
from .bitquery_stream import StreamRecord, stream_records_once
from .config import RadarConfig
from .credentials import CredentialConfig, load_credentials
from .http_json import BoundedJsonClient, HttpBoundaryError
from .evm_rpc import (
    HttpRpcClient,
    NATIVE_SPECS,
    RpcError,
    get_logs_with_413_split,
    native_log_block_number,
    parse_log_identity,
    parse_native_launch,
    probe_native_wss_once,
    stream_native_logs_once,
)
from .evm_token_flow import collect_creator_flow
from .models import Decision, FeatureSnapshot, ParseBatch, RadarEvent
from .narrative import NarrativeTracker
from .priority import PriorityTracker
from .runtime import RuntimeSettings, SlidingBudget, write_health
from .scoring import evaluate
from .sources import (
    ClankerPublicSource,
    DexScreenerSource,
    FomoPublicSource,
    GMGN_TRENDING_SOURCE,
    GmgnCliError,
    GmgnTokenInfoSnapshot,
    GmgnTokenInfoSource,
    GmgnTrendingResult,
    GmgnTrendingSnapshot,
    GmgnTrendingSource,
    GmgnTrenchesSource,
    GoPlusSource,
    MarketSnapshot,
    ProviderApiError,
    ProviderUnavailable,
)
from .storage import (
    claim_due_telegram,
    claim_due_enrichment,
    connect,
    enrichment_queue_under_pressure,
    enrichment_job_counts,
    EventIdentityConflict,
    enqueue_telegram,
    enqueue_observation_sample,
    finish_enrichment_job,
    get_runtime_state,
    initialize,
    mark_telegram_retry,
    mark_telegram_sent,
    reschedule_enrichment_job,
    schedule_enrichment_job,
    set_runtime_state,
    store_decision,
    store_event,
    store_provider_observation,
    store_raw_payload,
    telegram_outbox_counts,
)
from .telegram import TelegramClient, format_alert, format_observation_sample
from .unified_policy import (
    GmgnDiscoveryEvidence,
    GmgnSafetyEvidence,
    MIN_HOLDER_COUNT,
    MIN_MARKET_CAP_USD,
    UnifiedSafetyEvidence,
    any_buy_sell_ratio_at_least_one,
    gmgn_discovery_reasons,
    gmgn_safety_reasons,
    market_size_reasons,
    unified_safety_reasons,
)


def _log(kind: str, **fields) -> None:
    print(
        json.dumps(
            {"event": kind, **fields},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def _flag(payload: Dict[str, Any], key: str) -> Optional[bool]:
    value = payload.get(key)
    if value in ("1", 1, True):
        return True
    if value in ("0", 0, False):
        return False
    return None


def _fraction(payload: Dict[str, Any], key: str) -> Optional[float]:
    try:
        value = float(payload.get(key))
    except (TypeError, ValueError):
        return None
    if value < 0 or value != value or value == float("inf"):
        return None
    return value


def _security_features(payload: Dict[str, Any]) -> tuple[str, Optional[bool], str]:
    dangerous = (
        "is_honeypot",
        "cannot_sell_all",
        "is_blacklisted",
        "is_mintable",
        "hidden_owner",
        "selfdestruct",
        "external_call",
        "transfer_pausable",
    )
    flags = {key: _flag(payload, key) for key in dangerous}
    if any(value is True for value in flags.values()):
        security_status = "unsafe"
    elif flags["is_honeypot"] is False:
        security_status = "safe"
    else:
        security_status = "unknown"
    if flags["is_honeypot"] is True or flags["cannot_sell_all"] is True:
        sell_ok = False
    elif flags["is_honeypot"] is False:
        sell_ok = True
    else:
        sell_ok = None
    creator_values = [
        value
        for value in (
            _fraction(payload, "creator_percent"),
            _fraction(payload, "owner_percent"),
        )
        if value is not None
    ]
    if flags["is_blacklisted"] is True:
        creator_risk = "malicious"
    elif creator_values and max(creator_values) <= 0.10:
        creator_risk = "safe"
    else:
        creator_risk = "unknown"
    return security_status, sell_ok, creator_risk


class RadarDaemon:
    def __init__(
        self,
        settings: RuntimeSettings,
        credentials: CredentialConfig,
        config: RadarConfig = RadarConfig(),
    ) -> None:
        if settings.bitquery_enabled and not credentials.bitquery_token:
            raise RuntimeError("BITQUERY_TOKEN missing")
        if settings.fomo_enabled and not credentials.fomo_api_key:
            raise RuntimeError("FOMO_API_KEY missing")
        if not credentials.goplus_app_key or not credentials.goplus_app_secret:
            raise RuntimeError("GoPlus credentials missing")
        settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        settings.health_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings = settings
        self.credentials = credentials
        self.config = config
        self.connection = connect(settings.db_path)
        initialize(self.connection)
        self.stop_event = asyncio.Event()
        self.tracker = NarrativeTracker(config.narrative_window_ms)
        self.priority_tracker = PriorityTracker()
        self.goplus_budget = SlidingBudget(settings.enrich_max_per_hour)
        self.dex_budget = SlidingBudget(settings.enrich_max_per_hour)
        self.gmgn_budget = SlidingBudget(settings.gmgn_max_per_hour)
        self.gmgn_discovery_budget = SlidingBudget(
            settings.gmgn_discovery_max_per_hour
        )
        self.gmgn_call_lock = asyncio.Lock()
        self.gmgn_next_call_at = 0.0
        size_capacity = (
            settings.gmgn_max_per_hour
            if settings.gmgn_enabled
            else settings.enrich_max_per_hour
        )
        initial_total = max(1, size_capacity * 2 // 3)
        size_recheck_total = max(1, size_capacity - initial_total)
        bsc_initial = max(1, initial_total * 2 // 3)
        base_initial = max(1, initial_total // 4)
        initial_limits = {
            "bsc": bsc_initial,
            "base": base_initial,
            "robinhood": max(1, initial_total - bsc_initial - base_initial),
        }
        self.initial_chain_budgets = {
            chain: SlidingBudget(limit)
            for chain, limit in initial_limits.items()
        }
        self.initial_probe_budget = SlidingBudget(sum(initial_limits.values()))
        self.size_recheck_budget = SlidingBudget(size_recheck_total)
        self.dex_recheck_budget = SlidingBudget(
            max(1, settings.enrich_max_per_hour // 4)
        )
        self.telegram_budget = SlidingBudget(settings.telegram_max_per_hour)
        self.counters: Counter = Counter()
        self.last_success: Dict[str, int] = {}
        self.last_error: Dict[str, Dict] = {}
        self.fatal_task_error: Optional[BaseException] = None
        self.status = "starting"
        self.started_at_ms = int(time.time() * 1000)
        activation_key = "activation_ms:" + self.config.ruleset_version
        activation = get_runtime_state(self.connection, activation_key)
        if activation is None:
            activation = self.started_at_ms
            set_runtime_state(
                self.connection,
                activation_key,
                activation,
                self.started_at_ms,
            )
        self.unified_activation_ms = int(activation)
        self.bitquery_connected = False

        self.fomo = (
            FomoPublicSource(
                BoundedJsonClient(
                    allowed_hosts={"api.fomoapi.io"},
                    timeout_seconds=20,
                    max_bytes=512 * 1024,
                ),
                api_key=credentials.fomo_api_key,
            )
            if settings.fomo_enabled
            else None
        )
        self.clanker = ClankerPublicSource(
            BoundedJsonClient(
                allowed_hosts={"www.clanker.world"},
                timeout_seconds=20,
                max_bytes=512 * 1024,
            )
        )
        self.goplus = GoPlusSource(
            BoundedJsonClient(
                allowed_hosts={"api.gopluslabs.io"},
                timeout_seconds=20,
                max_bytes=512 * 1024,
            ),
            app_key=credentials.goplus_app_key,
            app_secret=credentials.goplus_app_secret,
        )
        self.dex = DexScreenerSource(
            BoundedJsonClient(
                allowed_hosts={"api.dexscreener.com"},
                timeout_seconds=15,
                max_bytes=512 * 1024,
            )
        )
        self.gmgn = (
            GmgnTrenchesSource(cache_seconds=settings.gmgn_cache_seconds)
            if settings.gmgn_enabled
            else None
        )
        self.gmgn_info = (
            GmgnTokenInfoSource(cache_seconds=settings.gmgn_cache_seconds)
            if settings.gmgn_enabled
            else None
        )
        self.gmgn_discovery = (
            GmgnTrendingSource()
            if settings.gmgn_discovery_enabled
            else None
        )
        self.telegram: Optional[TelegramClient] = None
        if settings.telegram_enabled:
            if not credentials.telegram_bot_token or not credentials.telegram_chat_id:
                raise RuntimeError("Telegram credentials missing")
            self.telegram = TelegramClient(
                BoundedJsonClient(
                    allowed_hosts={"api.telegram.org"},
                    timeout_seconds=15,
                    max_bytes=128 * 1024,
                ),
                bot_token=credentials.telegram_bot_token,
                chat_id=credentials.telegram_chat_id,
                topic_id=credentials.telegram_topic_id,
            )

        tokens = []
        if settings.bitquery_enabled:
            tokens.append(credentials.bitquery_token)
        if (
            settings.bitquery_enabled
            and credentials.bitquery_token_standby
            and credentials.bitquery_token_standby != credentials.bitquery_token
        ):
            tokens.append(credentials.bitquery_token_standby)
        self.bitquery_tokens = tuple(tokens)
        stored_slot = (
            get_runtime_state(
                self.connection,
                "bitquery_active_slot",
                "primary",
            )
            if tokens
            else "disabled"
        )
        self.bitquery_token_index = (
            1 if stored_slot == "standby" and len(tokens) > 1 else 0
        )
        self.bitquery_consecutive_errors = 0
        self.histories = tuple(
            BitqueryHistorySource(
                BoundedJsonClient(
                    allowed_hosts={"streaming.bitquery.io"},
                    timeout_seconds=25,
                    max_bytes=2 * 1024 * 1024,
                ),
                token,
            )
            for token in self.bitquery_tokens
        )
        self.native_endpoints: Dict[str, tuple] = {}
        self.native_http: Dict[str, tuple] = {}
        self.native_slot_index: Dict[str, int] = {}
        self.native_consecutive_errors: Dict[str, int] = {}
        self.native_connected: Dict[str, bool] = {}
        self.native_block_times: Dict[tuple, int] = {}
        self.native_reconnect_events: Dict[str, asyncio.Event] = {}
        self.native_primary_probe_successes: Dict[str, int] = {}
        self.native_checkpoint_hold: Dict[str, Dict[str, int]] = {}
        if settings.native_rpc_mode != "off":
            configured = {
                "bsc": (
                    (credentials.bsc_rpc_url, credentials.bsc_wss_url),
                    (
                        credentials.bsc_rpc_url_standby,
                        credentials.bsc_wss_url_standby,
                    ),
                ),
                "base": (
                    (credentials.base_rpc_url, credentials.base_wss_url),
                    (
                        credentials.base_rpc_url_standby,
                        credentials.base_wss_url_standby,
                    ),
                ),
                "robinhood": (
                    (
                        credentials.robinhood_rpc_url,
                        credentials.robinhood_wss_url,
                    ),
                    (
                        credentials.robinhood_rpc_url_standby,
                        credentials.robinhood_wss_url_standby,
                    ),
                ),
            }
            for chain, candidates in configured.items():
                pairs = []
                for http_url, wss_url in candidates:
                    if bool(http_url) != bool(wss_url):
                        raise RuntimeError("incomplete native RPC pair for " + chain)
                    if http_url:
                        pairs.append((http_url, wss_url))
                if not pairs:
                    raise RuntimeError("native RPC endpoints missing for " + chain)
                self.native_endpoints[chain] = tuple(pairs)
                self.native_http[chain] = tuple(
                    HttpRpcClient(http_url) for http_url, _ in pairs
                )
                stored_slot = get_runtime_state(
                    self.connection,
                    "native_rpc_slot:" + chain,
                    "primary",
                )
                self.native_slot_index[chain] = (
                    1 if stored_slot == "standby" and len(pairs) > 1 else 0
                )
                self.native_consecutive_errors[chain] = 0
                self.native_connected[chain] = False
                self.native_reconnect_events[chain] = asyncio.Event()
                self.native_primary_probe_successes[chain] = 0
                self.native_checkpoint_hold.pop(chain, None)

    async def run_gmgn_call(self, function, *args):
        async with self.gmgn_call_lock:
            delay = self.gmgn_next_call_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                return await asyncio.to_thread(function, *args)
            finally:
                self.gmgn_next_call_at = time.monotonic() + 3.0

    def cooldown_all_gmgn(self, seconds: float) -> None:
        self.gmgn_budget.cooldown(seconds)
        self.gmgn_discovery_budget.cooldown(seconds)

    def close(self) -> None:
        self.connection.close()

    def record_error(self, stage: str, exc: Exception) -> None:
        now = int(time.time() * 1000)
        sqlite_code = (
            getattr(exc, "sqlite_errorname", None)
            if isinstance(exc, sqlite3.Error)
            else None
        )
        self.counters["errors_total"] += 1
        self.counters["errors_" + stage] += 1
        self.last_error[stage] = {
            "at_ms": now,
            "type": type(exc).__name__,
            "code": (
                exc.code
                if isinstance(exc, (HttpBoundaryError, RpcError, GmgnCliError))
                else sqlite_code
            ),
            "status": exc.status
            if isinstance(exc, (HttpBoundaryError, RpcError, GmgnCliError))
            else None,
            "api_code": exc.api_code if isinstance(exc, ProviderApiError) else None,
        }

    def task_done(self, name: str, task: asyncio.Task) -> None:
        if self.stop_event.is_set() or task.cancelled():
            return
        error = task.exception()
        if error is None:
            error = RuntimeError(name + " task exited unexpectedly")
        self.record_error("task_" + name, error)
        _log(
            "MEME_RADAR_TASK_FATAL",
            task=name,
            error_type=type(error).__name__,
            error_code=getattr(error, "sqlite_errorname", None),
            no_broadcast=True,
        )
        self.fatal_task_error = error
        self.stop_event.set()

    def tracked_task(self, name: str, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name="meme-radar-" + name)
        task.add_done_callback(lambda result: self.task_done(name, result))
        return task

    def checkpoint_key(self, subscription_id: str) -> str:
        return "bitquery_checkpoint:" + subscription_id

    def bitquery_slot(self) -> str:
        if not self.bitquery_tokens:
            return "disabled"
        return "standby" if self.bitquery_token_index == 1 else "primary"

    def record_bitquery_failure(self, exc: Exception) -> None:
        self.bitquery_consecutive_errors += 1
        self.record_error("bitquery", exc)
        if (
            self.bitquery_token_index == 0
            and len(self.bitquery_tokens) > 1
            and self.bitquery_consecutive_errors >= 3
        ):
            self.bitquery_token_index = 1
            self.bitquery_consecutive_errors = 0
            self.counters["bitquery_failover_to_standby"] += 1
            set_runtime_state(
                self.connection,
                "bitquery_active_slot",
                "standby",
                int(time.time() * 1000),
            )

    def native_slot(self, chain: str) -> str:
        return "standby" if self.native_slot_index.get(chain, 0) == 1 else "primary"

    def record_native_failure(self, chain: str, exc: Exception) -> None:
        self.native_consecutive_errors[chain] += 1
        self.record_error("native_rpc_" + chain, exc)
        if (
            self.native_slot_index[chain] == 0
            and len(self.native_endpoints[chain]) > 1
            and self.native_consecutive_errors[chain] >= 3
        ):
            self.native_slot_index[chain] = 1
            self.native_consecutive_errors[chain] = 0
            self.counters["native_rpc_%s_failover" % chain] += 1
            set_runtime_state(
                self.connection,
                "native_rpc_slot:" + chain,
                "standby",
                int(time.time() * 1000),
            )

    def update_checkpoint(self, subscription_id: str, event: RadarEvent) -> None:
        key = self.checkpoint_key(subscription_id)
        current = int(get_runtime_state(self.connection, key, 0) or 0)
        value = max(current, event.source_published_at_ms)
        if value != current:
            set_runtime_state(
                self.connection,
                key,
                value,
                int(time.time() * 1000),
            )

    def candidate_eligible(self, source: str, event: RadarEvent) -> bool:
        source_age = event.observed_at_ms - event.source_published_at_ms
        token_age = event.token_age_ms
        if source == GMGN_TRENDING_SOURCE:
            return (
                self.settings.gmgn_discovery_enabled
                and event.source == GMGN_TRENDING_SOURCE
                and event.chain in {"bsc", "base", "solana"}
                and event.received_at_ms == event.observed_at_ms
                and token_age is not None
                and 0 <= token_age <= self.config.max_token_age_ms
            )
        expected_source = (
            "native_rpc"
            if self.settings.native_rpc_mode == "primary"
            else "bitquery"
        )
        return (
            not event.is_backfill
            and source == expected_source
            and event.chain in {"bsc", "base"}
            and 0 <= source_age <= self.config.max_source_age_ms
            and token_age is not None
            and 0 <= token_age <= self.config.max_enrichment_age_ms
        )

    def schedule_candidate(
        self,
        event: RadarEvent,
        features: FeatureSnapshot,
    ) -> None:
        priority = self.priority_tracker.score(event, features)
        queue_under_pressure = enrichment_queue_under_pressure(self.connection)
        if queue_under_pressure and priority.score < 35:
            self.counters["priority_admission_low"] += 1
            return
        has_narrative_signal = any(
            reason.startswith("NARRATIVE_BURST_") or reason == "CROSS_CHAIN_10"
            for reason in priority.reason_codes
        )
        if (
            priority.score < 60
            and event.source != GMGN_TRENDING_SOURCE
            and not has_narrative_signal
            and queue_under_pressure
        ):
            self.counters["priority_admission_pressure"] += 1
            return
        created_at_ms = event.token_created_at_ms or event.received_at_ms
        growth_source = event.source == GMGN_TRENDING_SOURCE
        earliest_due_ms = (
            event.received_at_ms
            if growth_source
            else max(
                event.received_at_ms,
                created_at_ms + self.config.size_recheck_ms[0],
            )
        )
        cohort_ms = self.config.priority_cohort_ms
        due_at_ms = (
            (earliest_due_ms + cohort_ms - 1) // cohort_ms
        ) * cohort_ms
        expires_at_ms = (
            event.received_at_ms + self.config.max_enrichment_age_ms
            if growth_source
            else created_at_ms + self.config.max_enrichment_age_ms
        )
        if expires_at_ms <= due_at_ms:
            self.counters["priority_expired_before_schedule"] += 1
            return
        if schedule_enrichment_job(
            self.connection,
            event=event,
            features=features,
            priority_version=self.config.priority_version,
            priority_score=priority.score,
            priority_reasons=priority.reason_codes,
            due_at_ms=due_at_ms,
            expires_at_ms=expires_at_ms,
        ):
            self.counters["priority_scheduled"] += 1
            band = "high" if priority.score >= 60 else "medium"
            self.counters["priority_" + band] += 1

    def process_batch(
        self,
        *,
        source: str,
        batch_id: str,
        payload: Dict,
        batch: ParseBatch,
        observed_at_ms: int,
        received_at_ms: int,
        checkpoint_source: Optional[str] = None,
    ) -> int:
        digest = store_raw_payload(
            self.connection,
            source=source,
            batch_id=batch_id,
            observed_at_ms=observed_at_ms,
            received_at_ms=received_at_ms,
            is_backfill=bool(
                batch.events and all(event.is_backfill for event in batch.events)
            ),
            payload=payload,
        )
        if digest != batch.raw_sha256:
            raise RuntimeError("raw payload digest mismatch")
        self.counters["parse_issues"] += len(batch.issues)
        inserted = 0
        for event in batch.events:
            if checkpoint_source:
                self.update_checkpoint(checkpoint_source, event)
            if not store_event(self.connection, event):
                self.counters["events_duplicate"] += 1
                continue
            inserted += 1
            self.counters["events_inserted"] += 1
            narrative = self.tracker.add(event)
            features = FeatureSnapshot(
                event_id=event.event_id,
                evaluated_at_ms=received_at_ms,
                metadata_complete=bool(event.name and event.symbol),
                narrative_burst=narrative.burst_count,
                cross_chain_count=narrative.cross_chain_count,
                hot_term_weight=narrative.hot_term_weight,
                quote_narrative=narrative.quote_narrative,
            )
            initial_config = replace(
                self.config,
                feature_version="features_v0_initial",
            )
            store_decision(
                self.connection,
                evaluate(event, features, initial_config),
            )
            if self.candidate_eligible(source, event):
                self.schedule_candidate(event, features)
        return inserted

    def native_checkpoint_key(self, chain: str) -> str:
        return "native_rpc_checkpoint:" + chain

    def hold_native_checkpoint(self, chain: str, block_number: int) -> None:
        """Stop advancing ``chain``'s checkpoint at an unprocessed block.

        The checkpoint means "every log below this block is durably stored", so
        it must not move past a log that failed processing. Holding stalls the
        chain instead of silently dropping the event; the hold is reported in
        health so the alert sidecar can escalate it.
        """
        now_ms = int(time.time() * 1000)
        held = self.native_checkpoint_hold.get(chain)
        if held is not None and int(held["block"]) <= block_number:
            held["holds"] = int(held["holds"]) + 1
            held["updated_at_ms"] = now_ms
        else:
            self.native_checkpoint_hold[chain] = {
                "block": int(block_number),
                "holds": 1,
                "since_ms": now_ms,
                "updated_at_ms": now_ms,
            }
        self.counters["native_rpc_%s_checkpoint_held" % chain] += 1

    def release_native_checkpoint(self, chain: str) -> None:
        if self.native_checkpoint_hold.pop(chain, None) is not None:
            self.counters["native_rpc_%s_checkpoint_released" % chain] += 1

    async def native_block_timestamp(self, chain: str, block_number: int) -> int:
        key = (chain, block_number)
        if key in self.native_block_times:
            return self.native_block_times[key]
        client = self.native_http[chain][self.native_slot_index[chain]]
        for attempt in range(3):
            try:
                value = await asyncio.to_thread(
                    client.block_timestamp_ms, block_number
                )
                break
            except RpcError as exc:
                if exc.code != "BLOCK_MISSING" or attempt == 2:
                    raise
                self.counters[
                    "native_rpc_%s_block_timestamp_retries" % chain
                ] += 1
                await asyncio.sleep(0.5 * (attempt + 1))
        self.native_block_times[key] = value
        if len(self.native_block_times) > 2048:
            self.native_block_times.pop(next(iter(self.native_block_times)))
        return value

    def record_token_metadata_error(self, field: str, error_type: str) -> None:
        self.counters["native_token_metadata_error_" + field] += 1
        self.last_error["native_token_metadata_" + field] = {
            "at_ms": int(time.time() * 1000),
            "type": error_type,
        }

    async def process_native_log(
        self,
        chain: str,
        log: Dict[str, Any],
        *,
        is_backfill: bool,
    ) -> int:
        spec = NATIVE_SPECS[chain]
        identity = parse_log_identity(log, spec)
        published_at_ms = await self.native_block_timestamp(
            chain,
            identity.block_number,
        )
        name = ""
        symbol = ""
        if self.settings.native_rpc_mode == "primary" and not is_backfill:
            client = self.native_http[chain][self.native_slot_index[chain]]
            name, symbol = await asyncio.to_thread(
                client.token_metadata,
                identity.token_address,
                self.record_token_metadata_error,
            )
        received_at_ms = int(time.time() * 1000)
        batch = parse_native_launch(
            log,
            spec,
            published_at_ms=published_at_ms,
            observed_at_ms=received_at_ms,
            received_at_ms=received_at_ms,
            name=name,
            symbol=symbol,
            is_backfill=is_backfill,
        )
        inserted = self.process_batch(
            source="native_rpc",
            batch_id="%s:%s:%d:%d"
            % (
                chain,
                identity.transaction_hash,
                identity.log_index,
                received_at_ms,
            ),
            payload={"chain": chain, "log": log},
            batch=batch,
            observed_at_ms=received_at_ms,
            received_at_ms=received_at_ms,
        )
        self.counters["native_rpc_%s_logs" % chain] += 1
        self.counters["native_rpc_%s_inserted" % chain] += inserted
        return inserted

    async def process_native_log_safe(
        self,
        chain: str,
        log: Dict[str, Any],
        *,
        is_backfill: bool,
    ) -> bool:
        """Report whether this log is durably stored, not whether it was new.

        The checkpoint may only advance over logs that are stored, so this
        answers exactly that question. A duplicate that storage already holds
        counts as stored; only a genuine persistence failure does not.
        """
        try:
            await self.process_native_log(
                chain,
                log,
                is_backfill=is_backfill,
            )
            return True
        except RpcError:
            raise
        except EventIdentityConflict:
            # One launch reaches us from both the live stream and backfill, and
            # only the live path fetches name and symbol, so the two renderings
            # of the same log differ and storage refuses the second one. The
            # event is already stored, so stalling the chain here would strand
            # a healthy checkpoint on a duplicate. Counted, not held.
            self.counters["native_rpc_%s_identity_conflict" % chain] += 1
            return True
        except (RuntimeError, ValueError, sqlite3.Error) as exc:
            self.record_error("native_processing_" + chain, exc)
            self.counters["native_rpc_%s_processing_rejected" % chain] += 1
            return False

    async def native_backfill(self, chain: str) -> None:
        spec = NATIVE_SPECS[chain]
        client = self.native_http[chain][self.native_slot_index[chain]]
        actual_chain = await asyncio.to_thread(client.chain_id)
        if actual_chain != spec.chain_id:
            raise RpcError("CHAIN_ID_MISMATCH")
        latest = await asyncio.to_thread(client.block_number)
        stored = int(
            get_runtime_state(
                self.connection,
                self.native_checkpoint_key(chain),
                max(
                    0,
                    latest - self.settings.native_rpc_backfill_blocks + 1,
                ),
            )
        )
        history_window = min(
            self.settings.native_rpc_backfill_blocks,
            spec.history_limit_blocks,
        )
        history_floor = max(0, latest - history_window + 1)
        start = max(history_floor, stored - 2)
        if stored < history_floor:
            self.counters["native_rpc_%s_backfill_truncated" % chain] += 1
        step = spec.max_log_blocks
        for lower in range(start, latest + 1, step):
            upper = min(latest, lower + step - 1)
            logs, split_count = await asyncio.to_thread(
                get_logs_with_413_split,
                client,
                spec,
                lower,
                upper,
            )
            self.counters["native_rpc_%s_backfill_413_splits" % chain] += split_count
            blocked_at = None
            for log in logs:
                processed = await self.process_native_log_safe(
                    chain,
                    log,
                    is_backfill=True,
                )
                if processed:
                    continue
                failed_at = native_log_block_number(log)
                failed_at = lower if failed_at is None else failed_at
                blocked_at = (
                    failed_at if blocked_at is None else min(blocked_at, failed_at)
                )
            self.counters["native_rpc_%s_backfill_calls" % chain] += 1
            if blocked_at is not None:
                self.hold_native_checkpoint(chain, blocked_at)
                return
        self.release_native_checkpoint(chain)
        set_runtime_state(
            self.connection,
            self.native_checkpoint_key(chain),
            latest,
            int(time.time() * 1000),
        )
        self.last_success["native_rpc_%s_backfill" % chain] = int(
            time.time() * 1000
        )

    async def native_rpc_loop(self, chain: str) -> None:
        backoff = 1
        while not self.stop_event.is_set():
            try:
                self.native_reconnect_events[chain].clear()
                await self.native_backfill(chain)
                if self.native_reconnect_events[chain].is_set():
                    continue

                def ready() -> None:
                    self.native_connected[chain] = True
                    self.last_success["native_rpc_%s_wss" % chain] = int(
                        time.time() * 1000
                    )

                endpoint = self.native_endpoints[chain][
                    self.native_slot_index[chain]
                ][1]
                async for log in stream_native_logs_once(
                    endpoint=endpoint,
                    spec=NATIVE_SPECS[chain],
                    stop_event=self.stop_event,
                    on_ready=ready,
                    reconnect_event=self.native_reconnect_events[chain],
                ):
                    if log is None:
                        await self.native_backfill(chain)
                        continue
                    if log.get("removed") is True:
                        self.counters["native_rpc_%s_removed" % chain] += 1
                        continue
                    identity = parse_log_identity(log, NATIVE_SPECS[chain])
                    processed = await self.process_native_log_safe(
                        chain,
                        log,
                        is_backfill=False,
                    )
                    if not processed:
                        self.hold_native_checkpoint(
                            chain,
                            identity.block_number,
                        )
                    elif chain not in self.native_checkpoint_hold:
                        set_runtime_state(
                            self.connection,
                            self.native_checkpoint_key(chain),
                            identity.block_number,
                            int(time.time() * 1000),
                        )
                    self.native_consecutive_errors[chain] = 0
                    self.last_success["native_rpc_%s_log" % chain] = int(
                        time.time() * 1000
                    )
                    backoff = 1
                self.native_connected[chain] = False
            except asyncio.CancelledError:
                self.native_connected[chain] = False
                raise
            except Exception as exc:
                self.native_connected[chain] = False
                self.record_native_failure(chain, exc)
                await self.wait_or_stop(backoff)
                backoff = min(backoff * 2, 60)

    async def probe_primary_once(self, chain: str) -> bool:
        if self.native_slot_index[chain] == 0:
            self.native_primary_probe_successes[chain] = 0
            return False
        primary_http = self.native_http[chain][0]
        try:
            actual_chain = await asyncio.to_thread(primary_http.chain_id)
            if actual_chain != NATIVE_SPECS[chain].chain_id:
                raise RpcError("CHAIN_ID_MISMATCH")
            await asyncio.to_thread(primary_http.block_number)
            await probe_native_wss_once(
                endpoint=self.native_endpoints[chain][0][1],
                spec=NATIVE_SPECS[chain],
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.native_primary_probe_successes[chain] = 0
            self.record_error("native_primary_probe_" + chain, exc)
            self.counters["native_rpc_%s_primary_probe_failed" % chain] += 1
            return False
        self.native_primary_probe_successes[chain] += 1
        self.last_success["native_rpc_%s_primary_probe" % chain] = int(
            time.time() * 1000
        )
        required = self.settings.native_rpc_primary_recovery_successes
        if self.native_primary_probe_successes[chain] < required:
            return False
        self.native_slot_index[chain] = 0
        self.native_primary_probe_successes[chain] = 0
        self.counters["native_rpc_%s_failback_primary" % chain] += 1
        set_runtime_state(
            self.connection,
            "native_rpc_slot:" + chain,
            "primary",
            int(time.time() * 1000),
        )
        self.native_reconnect_events[chain].set()
        return True

    async def native_primary_probe_loop(self, chain: str) -> None:
        while not self.stop_event.is_set():
            await self.wait_or_stop(
                self.settings.native_rpc_primary_probe_seconds
            )
            if self.stop_event.is_set():
                return
            await self.probe_primary_once(chain)

    async def backfill(self) -> None:
        now_ms = int(time.time() * 1000)
        floor = now_ms - self.settings.backfill_max_minutes * 60_000
        for source_name in ("fourmeme", "pons_events"):
            checkpoint = int(
                get_runtime_state(
                    self.connection,
                    self.checkpoint_key(source_name),
                    floor,
                )
                or floor
            )
            since_ms = max(floor, checkpoint - 5000)
            result, batch = await asyncio.to_thread(
                self.histories[self.bitquery_token_index].fetch,
                source_name,
                since_ms=since_ms,
                limit=1000,
            )
            data = result.data.get("data")
            self.process_batch(
                source="bitquery",
                batch_id="backfill:%s:%d" % (
                    source_name,
                    result.received_at_ms,
                ),
                payload=data,
                batch=batch,
                observed_at_ms=result.received_at_ms,
                received_at_ms=result.received_at_ms,
                checkpoint_source=source_name,
            )
            self.counters["backfill_batches"] += 1
            self.counters["backfill_events"] += len(batch.events)
            self.last_success["backfill_" + source_name] = result.received_at_ms

    def parse_stream_record(self, record: StreamRecord) -> ParseBatch:
        if record.subscription_id == "fourmeme":
            return parse_fourmeme(
                record.data,
                observed_at_ms=record.received_at_ms,
                received_at_ms=record.received_at_ms,
            )
        if record.subscription_id == "pons_events":
            return parse_pons_launched(
                record.data,
                observed_at_ms=record.received_at_ms,
                received_at_ms=record.received_at_ms,
            )
        raise ValueError("unknown live subscription")

    async def bitquery_loop(self) -> None:
        backoff = 1
        while not self.stop_event.is_set():
            try:
                await self.backfill()
                async for record in stream_records_once(
                    token=self.bitquery_tokens[self.bitquery_token_index],
                    stop_event=self.stop_event,
                ):
                    self.bitquery_connected = True
                    self.bitquery_consecutive_errors = 0
                    batch = self.parse_stream_record(record)
                    self.process_batch(
                        source="bitquery",
                        batch_id="live:%s:%d:%d"
                        % (
                            record.subscription_id,
                            record.received_at_ms,
                            self.counters["live_batches"],
                        ),
                        payload=record.data,
                        batch=batch,
                        observed_at_ms=record.received_at_ms,
                        received_at_ms=record.received_at_ms,
                        checkpoint_source=record.subscription_id,
                    )
                    self.counters["live_batches"] += 1
                    self.last_success[
                        "bitquery_" + record.subscription_id
                    ] = record.received_at_ms
                backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.bitquery_connected = False
                self.record_bitquery_failure(exc)
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=backoff
                    )
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 60)

    async def wait_or_stop(self, seconds: int) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def fomo_loop(self) -> None:
        if self.fomo is None:
            raise RuntimeError("FOMO source is disabled")
        while not self.stop_event.is_set():
            try:
                result, batch = await asyncio.to_thread(
                    self.fomo.fetch_alerts_with_result, 20
                )
                self.process_batch(
                    source="fomo",
                    batch_id="fomo:%d" % result.received_at_ms,
                    payload=result.data,
                    batch=batch,
                    observed_at_ms=result.received_at_ms,
                    received_at_ms=result.received_at_ms,
                )
                self.last_success["fomo"] = result.received_at_ms
                self.counters["fomo_polls"] += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.record_error("fomo", exc)
            await self.wait_or_stop(self.settings.fomo_poll_seconds)

    async def clanker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                meta, result, batch = await asyncio.to_thread(
                    self.clanker.fetch_latest_base_with_results, 20
                )
                store_raw_payload(
                    self.connection,
                    source="clanker_metadata",
                    batch_id="clanker-meta:%d" % meta.received_at_ms,
                    observed_at_ms=meta.received_at_ms,
                    received_at_ms=meta.received_at_ms,
                    is_backfill=False,
                    payload={"factories": meta.data},
                )
                self.process_batch(
                    source="clanker_public_api",
                    batch_id="clanker:%d" % result.received_at_ms,
                    payload=result.data,
                    batch=batch,
                    observed_at_ms=result.received_at_ms,
                    received_at_ms=result.received_at_ms,
                )
                self.last_success["clanker"] = result.received_at_ms
                self.counters["clanker_polls"] += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.record_error("clanker", exc)
            await self.wait_or_stop(self.settings.clanker_poll_seconds)

    def store_gmgn_discovery_snapshot(
        self,
        snapshot: GmgnTrendingSnapshot,
    ) -> None:
        summary = asdict(snapshot)
        store_provider_observation(
            self.connection,
            event_id=snapshot.event_id,
            provider="gmgn_trending",
            observed_at_ms=snapshot.observed_at_ms,
            status="ok",
            payload=summary,
            summary=summary,
        )
        size = GmgnTokenInfoSnapshot(
            chain=snapshot.chain,
            token_address=snapshot.token_address,
            observed_at_ms=snapshot.observed_at_ms,
            holder_count=snapshot.holder_count,
            market_cap_usd=snapshot.market_cap_usd,
            price_usd=None,
            liquidity_usd=snapshot.liquidity_usd,
            bytes_read=snapshot.bytes_read,
            elapsed_ms=snapshot.elapsed_ms,
        )
        size_summary = asdict(size)
        store_provider_observation(
            self.connection,
            event_id=snapshot.event_id,
            provider="gmgn_size",
            observed_at_ms=snapshot.observed_at_ms,
            status="ok",
            payload=size_summary,
            summary=size_summary,
        )

    def ingest_gmgn_discovery(
        self,
        chain: str,
        result: GmgnTrendingResult,
    ) -> int:
        new_events = tuple(
            event
            for event in result.batch.events
            if self.connection.execute(
                "SELECT 1 FROM radar_events WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
            is None
        )
        batch = replace(result.batch, events=new_events)
        inserted = self.process_batch(
            source=GMGN_TRENDING_SOURCE,
            batch_id="gmgn-trending:%s:%d" % (
                chain,
                result.observed_at_ms,
            ),
            payload=result.payload,
            batch=batch,
            observed_at_ms=result.observed_at_ms,
            received_at_ms=result.observed_at_ms,
        )
        for snapshot in result.snapshots:
            self.store_gmgn_discovery_snapshot(snapshot)
        self.counters["gmgn_discovery_rows"] += len(result.snapshots)
        self.counters["gmgn_discovery_inserted"] += inserted
        return inserted

    async def gmgn_discovery_loop(self) -> None:
        if self.gmgn_discovery is None:
            return
        chains = ("bsc", "base", "solana")
        while not self.stop_event.is_set():
            for chain in chains:
                if not self.gmgn_discovery_budget.take():
                    self.counters["gmgn_discovery_budget_blocked"] += 1
                    break
                try:
                    result = await self.run_gmgn_call(
                        self.gmgn_discovery.fetch,
                        chain,
                    )
                    self.ingest_gmgn_discovery(chain, result)
                    self.last_success["gmgn_discovery"] = (
                        result.observed_at_ms
                    )
                    self.last_success["gmgn_discovery_" + chain] = (
                        result.observed_at_ms
                    )
                    self.last_error.pop(
                        "gmgn_discovery_" + chain,
                        None,
                    )
                    self.counters["gmgn_discovery_polls_" + chain] += 1
                except asyncio.CancelledError:
                    raise
                except GmgnCliError as exc:
                    self.record_error("gmgn_discovery_" + chain, exc)
                    if exc.status == 429:
                        self.cooldown_all_gmgn(300)
                        self.counters["gmgn_discovery_429_cooldown"] += 1
                        break
                    elif exc.status in {401, 403}:
                        self.cooldown_all_gmgn(1800)
                        self.counters["gmgn_discovery_auth_cooldown"] += 1
                        break
                    elif exc.code.startswith("CONFIG_CHECK"):
                        self.cooldown_all_gmgn(1800)
                        self.counters["gmgn_discovery_config_cooldown"] += 1
                        break
                    self.counters["gmgn_discovery_chain_errors"] += 1
                except (OSError, TypeError, ValueError) as exc:
                    self.record_error("gmgn_discovery_" + chain, exc)
                    self.counters["gmgn_discovery_chain_errors"] += 1
            await self.wait_or_stop(
                self.settings.gmgn_discovery_poll_seconds
            )

    def provider_failure(
        self,
        event: RadarEvent,
        provider: str,
        exc: Exception,
    ) -> None:
        if isinstance(exc, HttpBoundaryError) and exc.status == 429:
            if provider == "goplus":
                self.goplus_budget.cooldown(60)
            else:
                self.dex_budget.cooldown(60)
            self.counters[provider + "_429"] += 1
        if (
            provider == "goplus"
            and isinstance(exc, ProviderApiError)
            and exc.api_code == 4029
        ):
            self.goplus_budget.cooldown(300)
            self.counters["goplus_4029_cooldown"] += 1
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider=provider,
            observed_at_ms=int(time.time() * 1000),
            status="error",
            payload={},
            summary={},
            error_code=type(exc).__name__,
        )
        self.record_error(provider, exc)

    def provider_unavailable(
        self,
        event: RadarEvent,
        provider: str,
    ) -> None:
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider=provider,
            observed_at_ms=int(time.time() * 1000),
            status="unavailable",
            payload={},
            summary={},
            error_code="NOT_COVERED",
        )
        self.counters[provider + "_unavailable"] += 1

    @staticmethod
    def market_summary(market: MarketSnapshot) -> Dict[str, Any]:
        return asdict(market)

    def finish_job(self, job: Dict[str, Any], code: str, now_ms: int) -> None:
        finish_enrichment_job(
            self.connection,
            job,
            result_code=code,
            now_ms=now_ms,
        )
        self.counters["enrichment_finished"] += 1
        self.counters["enrichment_result_" + code.lower()] += 1

    def reschedule_job(
        self,
        job: Dict[str, Any],
        *,
        stage: str,
        due_at_ms: int,
        dex_attempts: int,
        security_attempts: int,
        market: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        if due_at_ms >= int(job["expires_at_ms"]):
            self.finish_job(job, "EXPIRED", now_ms)
            return
        reschedule_enrichment_job(
            self.connection,
            job,
            stage=stage,
            dex_attempts=dex_attempts,
            security_attempts=security_attempts,
            due_at_ms=due_at_ms,
            now_ms=now_ms,
            market=market,
            last_error=error,
        )
        self.counters["enrichment_rescheduled"] += 1

    def retry_market(
        self,
        job: Dict[str, Any],
        attempts: int,
        now_ms: int,
        error: str,
    ) -> None:
        if attempts >= len(self.config.enrichment_delay_ms):
            self.finish_job(job, error, now_ms)
            return
        event = job["event"]
        anchor = event.token_created_at_ms or event.received_at_ms
        due_at_ms = max(
            now_ms + self.config.enrichment_min_retry_ms[attempts],
            anchor + self.config.enrichment_delay_ms[attempts],
        )
        self.reschedule_job(
            job,
            stage="dex_recheck",
            due_at_ms=due_at_ms,
            dex_attempts=attempts,
            security_attempts=int(job["security_attempts"]),
            error=error,
        )

    def gmgn_size_observation(
        self,
        event: RadarEvent,
    ) -> Optional[GmgnTokenInfoSnapshot]:
        row = self.connection.execute(
            """
            SELECT summary_json FROM provider_observations
            WHERE event_id=? AND provider='gmgn_size' AND status='ok'
            ORDER BY observed_at_ms DESC LIMIT 1
            """,
            (event.event_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            return GmgnTokenInfoSnapshot(**json.loads(row["summary_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def gmgn_discovery_observation(
        self,
        event: RadarEvent,
    ) -> Optional[GmgnTrendingSnapshot]:
        row = self.connection.execute(
            """
            SELECT summary_json FROM provider_observations
            WHERE event_id=? AND provider='gmgn_trending' AND status='ok'
            ORDER BY observed_at_ms DESC LIMIT 1
            """,
            (event.event_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            return GmgnTrendingSnapshot(**json.loads(row["summary_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def has_gmgn_size_attempt(self, event: RadarEvent) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM provider_observations
            WHERE event_id=? AND provider='gmgn_size' LIMIT 1
            """,
            (event.event_id,),
        ).fetchone()
        return row is not None

    def next_size_check_ms(
        self,
        event: RadarEvent,
        now_ms: int,
        snapshot: Optional[GmgnTokenInfoSnapshot] = None,
    ) -> Optional[int]:
        anchor = event.token_created_at_ms or event.received_at_ms
        offsets = self.config.size_recheck_ms
        if snapshot is not None:
            cap = snapshot.market_cap_usd
            holders = snapshot.holder_count
            if cap is not None and holders is not None:
                if cap < 20_000 and holders < 20:
                    offsets = ()
                elif cap < 60_000 and holders < 60:
                    offsets = tuple(
                        offset
                        for offset in offsets
                        if 10 * 60_000 <= offset <= 20 * 60_000
                    )
        for offset in offsets:
            due_at_ms = anchor + offset
            if due_at_ms > now_ms:
                return due_at_ms
        return None

    def reschedule_size_check(
        self,
        job: Dict[str, Any],
        now_ms: int,
        final_code: str,
        snapshot: Optional[GmgnTokenInfoSnapshot] = None,
    ) -> None:
        due_at_ms = self.next_size_check_ms(job["event"], now_ms, snapshot)
        if due_at_ms is None:
            self.finish_job(job, final_code, now_ms)
            return
        self.reschedule_job(
            job,
            stage="dex_probe",
            due_at_ms=due_at_ms,
            dex_attempts=int(job["dex_attempts"]),
            security_attempts=int(job["security_attempts"]),
            error="GMGN_SIZE_RECHECK",
        )
        self.counters["market_size_recheck_scheduled"] += 1

    async def gmgn_size_prefilter(
        self,
        job: Dict[str, Any],
        now_ms: int,
    ) -> Optional[GmgnTokenInfoSnapshot]:
        event = job["event"]
        if self.gmgn_info is None:
            return None
        if not self.gmgn_info.has_fresh_cache(event.chain, event.token_address):
            if not self.gmgn_budget.take():
                self.counters["gmgn_size_budget_deferred"] += 1
                store_provider_observation(
                    self.connection,
                    event_id=event.event_id,
                    provider="gmgn_size",
                    observed_at_ms=now_ms,
                    status="unavailable",
                    payload={},
                    summary={},
                    error_code="BUDGET",
                )
                self.reschedule_size_check(
                    job, now_ms, "GMGN_SIZE_BUDGET_EXHAUSTED"
                )
                return None
        try:
            snapshot = await self.run_gmgn_call(
                self.gmgn_info.token_info,
                event.chain,
                event.token_address,
            )
        except (GmgnCliError, OSError, ValueError) as exc:
            self.provider_failure(event, "gmgn_size", exc)
            if isinstance(exc, GmgnCliError) and exc.status == 429:
                self.cooldown_all_gmgn(300)
            self.reschedule_size_check(job, now_ms, "GMGN_SIZE_ERROR")
            return None
        if snapshot is None:
            self.provider_unavailable(event, "gmgn_size")
            self.reschedule_size_check(job, now_ms, "GMGN_SIZE_UNAVAILABLE")
            return None
        summary = asdict(snapshot)
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="gmgn_size",
            observed_at_ms=snapshot.observed_at_ms,
            status="ok",
            payload=summary,
            summary=summary,
        )
        self.last_success["gmgn_size"] = snapshot.observed_at_ms
        reasons = market_size_reasons(
            snapshot.market_cap_usd,
            snapshot.holder_count,
        )
        if reasons:
            for reason in reasons:
                self.counters["size_" + reason] += 1
            self.reschedule_size_check(
                job,
                now_ms,
                "MARKET_SIZE_BLOCKED",
                snapshot,
            )
            return None
        self.counters["market_size_pass"] += 1
        return snapshot

    @staticmethod
    def apply_gmgn_size(
        market: MarketSnapshot,
        snapshot: Optional[GmgnTokenInfoSnapshot],
    ) -> MarketSnapshot:
        if snapshot is None:
            return market
        return replace(
            market,
            price_usd=(
                market.price_usd
                if market.price_usd is not None
                else snapshot.price_usd
            ),
            liquidity_usd=(
                market.liquidity_usd
                if market.liquidity_usd is not None
                else snapshot.liquidity_usd
            ),
            market_cap_usd=(
                market.market_cap_usd
                if market.market_cap_usd is not None
                else snapshot.market_cap_usd
            ),
            holder_count=snapshot.holder_count,
        )

    async def process_market_stage(self, job: Dict[str, Any]) -> None:
        now_ms = int(time.time() * 1000)
        event = job["event"]
        if event.received_at_ms < self.unified_activation_ms:
            self.finish_job(job, "UNIFIED_PRE_ACTIVATION", now_ms)
            self.counters["unified_pre_activation"] += 1
            return
        size_snapshot = self.gmgn_size_observation(event)
        if (
            job["stage"] != "dex_probe"
            and self.gmgn_info is not None
            and size_snapshot is None
        ):
            self.finish_job(job, "GMGN_SIZE_STATE_MISSING", now_ms)
            return
        if job["stage"] == "dex_probe":
            has_size_attempt = self.has_gmgn_size_attempt(event)
            if not has_size_attempt:
                chain_budget = self.initial_chain_budgets[event.chain]
                if (
                    not self.initial_probe_budget.take()
                    or not chain_budget.take()
                ):
                    self.counters["initial_probe_budget_deferred"] += 1
                    self.reschedule_job(
                        job,
                        stage="dex_probe",
                        due_at_ms=now_ms + 30_000,
                        dex_attempts=int(job["dex_attempts"]),
                        security_attempts=int(job["security_attempts"]),
                    )
                    return
            elif not self.size_recheck_budget.take():
                self.counters["size_recheck_budget_deferred"] += 1
                self.reschedule_job(
                    job,
                    stage="dex_probe",
                    due_at_ms=now_ms + 30_000,
                    dex_attempts=int(job["dex_attempts"]),
                    security_attempts=int(job["security_attempts"]),
                )
                return
            if (
                self.gmgn_info is not None
                and event.source != GMGN_TRENDING_SOURCE
            ):
                size_snapshot = await self.gmgn_size_prefilter(job, now_ms)
                if size_snapshot is None:
                    return
            if event.source == GMGN_TRENDING_SOURCE:
                reasons = (
                    ("gmgn_discovery_size_missing",)
                    if size_snapshot is None
                    else market_size_reasons(
                        size_snapshot.market_cap_usd,
                        size_snapshot.holder_count,
                    )
                )
                if reasons:
                    for reason in reasons:
                        self.counters["gmgn_discovery_" + reason] += 1
                    self.finish_job(
                        job,
                        "GMGN_DISCOVERY_SIZE_BLOCKED",
                        now_ms,
                    )
                    return
        if (
            job["stage"] == "dex_recheck"
            and not self.dex_recheck_budget.take()
        ):
            self.counters["dex_recheck_budget_deferred"] += 1
            self.reschedule_job(
                job,
                stage="dex_recheck",
                due_at_ms=now_ms + 30_000,
                dex_attempts=int(job["dex_attempts"]),
                security_attempts=int(job["security_attempts"]),
                market=json.loads(job["market_json"] or "null"),
            )
            return
        if not self.dex_budget.take():
            self.counters["dex_budget_deferred"] += 1
            self.reschedule_job(
                job,
                stage=(
                    "dex_recheck"
                    if size_snapshot is not None
                    else str(job["stage"])
                ),
                due_at_ms=now_ms + 30_000,
                dex_attempts=int(job["dex_attempts"]),
                security_attempts=int(job["security_attempts"]),
            )
            return
        attempts = int(job["dex_attempts"]) + 1
        try:
            market = await asyncio.to_thread(
                self.dex.token_market,
                event.chain,
                event.token_address,
            )
        except Exception as exc:
            self.provider_failure(event, "dexscreener", exc)
            self.retry_market(
                job,
                attempts,
                int(time.time() * 1000),
                "DEX_ERROR",
            )
            return
        completed_at_ms = max(
            int(time.time() * 1000),
            market.observed_at_ms,
        )
        dex_summary = self.market_summary(market)
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="dexscreener",
            observed_at_ms=market.observed_at_ms,
            status="ok" if market.pair_count else "unavailable",
            payload=dex_summary,
            summary=dex_summary,
        )
        self.last_success["dexscreener"] = market.observed_at_ms
        if not market.pair_count:
            self.counters["dex_unavailable"] += 1
            self.retry_market(
                job,
                attempts,
                completed_at_ms,
                "DEX_UNAVAILABLE",
            )
            return
        market = self.apply_gmgn_size(market, size_snapshot)
        summary = self.market_summary(market)
        if not self.market_ready_for_enrichment(event, market):
            self.counters["dex_market_incomplete"] += 1
            self.retry_market(
                job,
                attempts,
                completed_at_ms,
                "DEX_MARKET_INCOMPLETE",
            )
            return
        self.counters["dex_ok"] += 1
        anchor = event.token_created_at_ms or event.received_at_ms
        mature_at = anchor + self.config.enrichment_delay_ms[1]
        if attempts == 1 and completed_at_ms < mature_at:
            self.reschedule_job(
                job,
                stage="dex_recheck",
                due_at_ms=mature_at,
                dex_attempts=attempts,
                security_attempts=int(job["security_attempts"]),
                market=summary,
            )
            return
        self.reschedule_job(
            job,
            stage="security_check",
            due_at_ms=completed_at_ms + 1,
            dex_attempts=attempts,
            security_attempts=int(job["security_attempts"]),
            market=summary,
        )

    async def process_security_stage(self, job: Dict[str, Any]) -> None:
        now_ms = int(time.time() * 1000)
        event = job["event"]
        market_data = json.loads(job["market_json"] or "null")
        if not isinstance(market_data, dict):
            self.finish_job(job, "MARKET_STATE_MISSING", now_ms)
            return
        market = MarketSnapshot(**market_data)
        if now_ms - market.observed_at_ms > self.config.max_price_age_ms:
            self.retry_market(
                job,
                int(job["dex_attempts"]),
                now_ms,
                "PRICE_STALE",
            )
            return
        market = await self.unified_main_market(job, market, now_ms)
        if market is None:
            return
        if event.source == GMGN_TRENDING_SOURCE and event.chain == "solana":
            self.complete_gmgn_solana_security(job, market, now_ms)
            return
        if not self.goplus_budget.take():
            cooldown = self.goplus_budget.snapshot()["cooldown_seconds"]
            self.counters["goplus_budget_deferred"] += 1
            self.reschedule_job(
                job,
                stage="security_check",
                due_at_ms=now_ms + max(30, cooldown) * 1000,
                dex_attempts=int(job["dex_attempts"]),
                security_attempts=int(job["security_attempts"]),
                market=market_data,
            )
            return
        await self.complete_security(job, market)

    def complete_gmgn_solana_security(
        self,
        job: Dict[str, Any],
        market: MarketSnapshot,
        now_ms: int,
    ) -> None:
        event = job["event"]
        snapshot = self.gmgn_discovery_observation(event)
        if snapshot is None:
            self.finish_job(job, "GMGN_SOLANA_SECURITY_MISSING", now_ms)
            return
        summary = asdict(snapshot)
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="gmgn_solana_security",
            observed_at_ms=now_ms,
            status="ok",
            payload=summary,
            summary=summary,
        )
        decision = self.finalize_enrichment(
            event,
            job["features"],
            market,
            summary,
            now_ms,
            security_override=("safe", True, "safe"),
        )
        self.counters["gmgn_solana_security_ok"] += 1
        self.last_success["gmgn_solana_security"] = now_ms
        self.finish_job(job, "FINAL_" + decision.delivery.upper(), now_ms)

    def native_launch_block(self, event: RadarEvent) -> int:
        row = self.connection.execute(
            "SELECT payload_json FROM raw_payloads WHERE raw_sha256=?",
            (event.raw_sha256,),
        ).fetchone()
        if row is None:
            raise ValueError("native launch payload missing")
        payload = json.loads(row["payload_json"])
        log = payload.get("log") if isinstance(payload, dict) else None
        value = log.get("blockNumber") if isinstance(log, dict) else None
        if not isinstance(value, str) or not value.startswith("0x"):
            raise ValueError("native launch block missing")
        return int(value, 16)

    def identity_market_rank(
        self,
        event: RadarEvent,
        market_cap_usd: float,
        now_ms: int,
    ) -> tuple:
        name = " ".join(event.name.casefold().strip().split())
        symbol = " ".join(event.symbol.casefold().strip().split())
        rows = self.connection.execute(
            """
            SELECT DISTINCT token_address FROM radar_events
            WHERE chain=? AND event_kind='launch' AND received_at_ms>=?
              AND ((?<>'' AND lower(trim(json_extract(event_json,'$.name')))=?)
                OR (?<>'' AND lower(trim(json_extract(event_json,'$.symbol')))=?))
            """,
            (
                event.chain,
                now_ms - 900_000,
                name,
                name,
                symbol,
                symbol,
            ),
        ).fetchall()
        normalize_token = (
            (lambda value: str(value))
            if event.chain == "solana"
            else (lambda value: str(value).lower())
        )
        tokens = {normalize_token(row[0]) for row in rows}
        tokens.add(event.token_address)
        values = {event.token_address: market_cap_usd}
        for token in tokens - {event.token_address}:
            row = self.connection.execute(
                """
                SELECT p.summary_json FROM provider_observations p
                JOIN radar_events e ON e.event_id=p.event_id
                WHERE e.chain=? AND e.token_address=?
                  AND p.provider IN ('dexscreener','gmgn_trending')
                  AND p.status='ok'
                ORDER BY CASE p.provider WHEN 'dexscreener' THEN 0 ELSE 1 END,
                         p.observed_at_ms DESC LIMIT 1
                """,
                (event.chain, token),
            ).fetchone()
            if row is None:
                return len(tokens), None
            try:
                summary = json.loads(row["summary_json"])
                value = summary.get("market_cap_usd")
                if value is None:
                    value = summary.get("fdv_usd")
                values[token] = float(value)
            except (TypeError, ValueError, json.JSONDecodeError):
                return len(tokens), None
        ranked = sorted(values, key=lambda token: (values[token], token), reverse=True)
        return len(tokens), ranked.index(event.token_address) + 1

    def gmgn_discovery_market(
        self,
        job: Dict[str, Any],
        market: MarketSnapshot,
        now_ms: int,
    ) -> Optional[MarketSnapshot]:
        event = job["event"]
        snapshot = self.gmgn_discovery_observation(event)
        reasons = []
        if snapshot is None:
            reasons.append("gmgn_discovery_evidence_missing")
        elif (
            snapshot.chain != event.chain
            or snapshot.token_address != event.token_address
        ):
            reasons.append("gmgn_discovery_identity_mismatch")
        elif now_ms - snapshot.observed_at_ms < -self.config.max_future_skew_ms:
            reasons.append("gmgn_discovery_evidence_in_future")
        elif now_ms - snapshot.observed_at_ms > self.config.max_source_age_ms:
            reasons.append("gmgn_discovery_evidence_stale")
        elif not event.creator:
            reasons.append("gmgn_discovery_creator_unknown")
        else:
            reasons.extend(
                gmgn_discovery_reasons(
                    GmgnDiscoveryEvidence(
                        chain=snapshot.chain,
                        market_cap_usd=snapshot.market_cap_usd,
                        holder_count=snapshot.holder_count,
                        liquidity_usd=snapshot.liquidity_usd,
                        volume_5m_usd=snapshot.volume_5m_usd,
                        swaps_5m=snapshot.swaps_5m,
                        buys_5m=snapshot.buys_5m,
                        sells_5m=snapshot.sells_5m,
                        smart_degen_count=snapshot.smart_degen_count,
                        creator_close=snapshot.creator_close,
                        is_honeypot=snapshot.is_honeypot,
                        is_open_source=snapshot.is_open_source,
                        owner_renounced=snapshot.owner_renounced,
                        renounced_mint=snapshot.renounced_mint,
                        renounced_freeze=snapshot.renounced_freeze,
                        is_wash_trading=snapshot.is_wash_trading,
                        rug_ratio=snapshot.rug_ratio,
                        top_10_holder_rate=snapshot.top_10_holder_rate,
                        bundler_rate=snapshot.bundler_rate,
                        insider_rate=snapshot.insider_rate,
                        entrapment_ratio=snapshot.entrapment_ratio,
                        dev_team_hold_rate=snapshot.dev_team_hold_rate,
                    )
                )
            )
        buys = market.buy_transactions_5m
        sells = market.sell_transactions_5m
        volume = market.volume_5m_usd
        if buys is None or sells is None or volume is None:
            reasons.append("market_activity_unknown")
        else:
            if buys < 10:
                reasons.append("external_buys_below_min")
            if sells < 1:
                reasons.append("sells_below_min")
            if volume < 10:
                reasons.append("volume_5m_below_min")
        gmgn_pair = (
            (None, None)
            if snapshot is None
            else (snapshot.buys_5m, snapshot.sells_5m)
        )
        if not any_buy_sell_ratio_at_least_one(
            gmgn_pair,
            (buys, sells),
        ):
            reasons.append("combined_buy_sell_ratio_below_one")
        market_cap = (
            market.market_cap_usd
            if market.market_cap_usd is not None
            else market.fdv_usd
        )
        identity_count = 1
        identity_rank = 1
        if market_cap is None:
            reasons.append("market_cap_unknown")
        else:
            identity_count, identity_rank = self.identity_market_rank(
                event,
                float(market_cap),
                now_ms,
            )
            if identity_count > 1 and identity_rank is None:
                reasons.append("identity_market_cap_incomplete")
            elif identity_rank != 1:
                reasons.append("identity_lower_market_cap")
        summary = {
            "source": "gmgn_trending",
            "market_cap_usd": market_cap,
            "holder_count": None if snapshot is None else snapshot.holder_count,
            "creator_close": None if snapshot is None else snapshot.creator_close,
            "smart_degen_count": (
                None if snapshot is None else snapshot.smart_degen_count
            ),
            "identity_count": identity_count,
            "identity_market_cap_rank": identity_rank,
            "reason_codes": sorted(set(reasons)),
        }
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="unified_gate",
            observed_at_ms=now_ms,
            status="ok",
            payload=summary,
            summary=summary,
        )
        if reasons:
            for reason in set(reasons):
                counter = (
                    reason
                    if reason.startswith("gmgn_discovery_")
                    else "gmgn_discovery_" + reason
                )
                self.counters[counter] += 1
            self.finish_job(job, "GMGN_DISCOVERY_BLOCKED", now_ms)
            return None
        self.counters["gmgn_discovery_gate_pass"] += 1
        return replace(
            market,
            holder_count=snapshot.holder_count,
            identity_count=identity_count,
            identity_market_cap_rank=identity_rank,
            external_buy_transactions_5m=buys,
        )

    async def unified_main_market(
        self,
        job: Dict[str, Any],
        market: MarketSnapshot,
        now_ms: int,
    ) -> Optional[MarketSnapshot]:
        event = job["event"]
        if event.source == GMGN_TRENDING_SOURCE:
            return self.gmgn_discovery_market(job, market, now_ms)
        if (
            event.chain not in {"bsc", "base"}
            or self.settings.native_rpc_mode != "primary"
        ):
            return market
        if event.received_at_ms < self.unified_activation_ms:
            self.finish_job(job, "UNIFIED_PRE_ACTIVATION", now_ms)
            self.counters["unified_pre_activation"] += 1
            return None
        market_cap = (
            market.market_cap_usd
            if market.market_cap_usd is not None
            else market.fdv_usd
        )
        if market_cap is None or not event.creator:
            self.finish_job(job, "UNIFIED_EVIDENCE_INCOMPLETE", now_ms)
            self.counters["unified_evidence_incomplete"] += 1
            return None
        try:
            launch_block = self.native_launch_block(event)
            client = self.native_http[event.chain][
                self.native_slot_index[event.chain]
            ]
            latest = await asyncio.to_thread(client.block_number)
            spec = NATIVE_SPECS[event.chain]
            flow = await asyncio.to_thread(
                collect_creator_flow,
                client,
                token_address=event.token_address,
                creator_address=event.creator,
                from_block=launch_block,
                to_block=latest,
                chunk_blocks=spec.max_log_blocks,
                max_span_blocks=spec.history_limit_blocks,
            )
            identity_count, identity_rank = self.identity_market_rank(
                event, float(market_cap), now_ms
            )
        except (OSError, RpcError, TypeError, ValueError, json.JSONDecodeError):
            self.finish_job(job, "UNIFIED_EVIDENCE_INCOMPLETE", now_ms)
            self.counters["unified_evidence_incomplete"] += 1
            return None
        retention = flow.retention_ratio
        evidence = UnifiedSafetyEvidence(
            market_cap_usd=float(market_cap),
            creator_inbound_count=flow.inbound_count,
            creator_outbound_count=flow.outbound_count,
            creator_retention_ratio=(
                None if retention is None else min(1.0, retention)
            ),
            creator_balance_present=flow.balance_raw > 0,
            identity_count=identity_count,
            identity_market_cap_rank=identity_rank,
        )
        reasons = list(unified_safety_reasons(evidence))
        buys = market.buy_transactions_5m
        sells = market.sell_transactions_5m
        external_buys = None if buys is None else max(0, buys - flow.inbound_count)
        if external_buys is None or sells is None:
            reasons.append("market_activity_unknown")
        else:
            if external_buys < 10:
                reasons.append("external_buys_below_min")
            if external_buys / max(1, sells) < 1.5:
                reasons.append("buy_sell_ratio_below_min")
        if market.volume_5m_usd is None or market.volume_5m_usd < 10:
            reasons.append("volume_5m_below_min")
        features = job["features"]
        if features.narrative_burst < 3 and features.cross_chain_count < 2:
            reasons.append("narrative_below_gate")
        if not reasons:
            reasons.extend(await self.gmgn_gate(event, now_ms))
        summary = {
            "market_cap_usd": float(market_cap),
            "creator_inbound_count": flow.inbound_count,
            "creator_outbound_count": flow.outbound_count,
            "creator_retention_ratio": retention,
            "creator_balance_raw": str(flow.balance_raw),
            "external_buys_5m": external_buys,
            "sells_5m": sells,
            "identity_count": identity_count,
            "identity_market_cap_rank": identity_rank,
            "reason_codes": sorted(set(reasons)),
        }
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="unified_gate",
            observed_at_ms=now_ms,
            status="ok",
            payload=summary,
            summary=summary,
        )
        if reasons:
            for reason in set(reasons):
                self.counters["unified_" + reason] += 1
            self.finish_job(job, "UNIFIED_BLOCKED", now_ms)
            return None
        self.counters["unified_pass"] += 1
        return replace(
            market,
            creator_inbound_count=flow.inbound_count,
            creator_outbound_count=flow.outbound_count,
            creator_retention_ratio=(
                None if retention is None else min(1.0, retention)
            ),
            identity_count=identity_count,
            identity_market_cap_rank=identity_rank,
            external_buy_transactions_5m=external_buys,
        )

    async def gmgn_gate(self, event: RadarEvent, now_ms: int) -> list:
        if self.gmgn is None:
            return []
        if not self.gmgn.has_fresh_cache(event.chain):
            if not self.gmgn_budget.take():
                self.counters["gmgn_budget_deferred"] += 1
                return []
        try:
            snapshot = await self.run_gmgn_call(
                self.gmgn.token_safety,
                event.chain,
                event.token_address,
            )
        except GmgnCliError as exc:
            self.provider_failure(event, "gmgn", exc)
            if exc.status == 429:
                self.cooldown_all_gmgn(300)
                self.counters["gmgn_429_cooldown"] += 1
            elif exc.status in {401, 403}:
                self.cooldown_all_gmgn(1800)
                self.counters["gmgn_auth_cooldown"] += 1
            return []
        if snapshot is None:
            self.provider_unavailable(event, "gmgn")
            self.counters["gmgn_coverage_miss"] += 1
            return []
        summary = asdict(snapshot)
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="gmgn",
            observed_at_ms=snapshot.observed_at_ms,
            status="ok",
            payload=summary,
            summary=summary,
        )
        self.counters["gmgn_ok"] += 1
        self.last_success["gmgn"] = snapshot.observed_at_ms
        return list(
            gmgn_safety_reasons(
                GmgnSafetyEvidence(
                    market_cap_usd=snapshot.market_cap_usd,
                    creator_token_status=snapshot.creator_token_status,
                    is_honeypot=snapshot.is_honeypot,
                    is_open_source=snapshot.is_open_source,
                    owner_renounced=snapshot.owner_renounced,
                    is_wash_trading=snapshot.is_wash_trading,
                    rug_ratio=snapshot.rug_ratio,
                    top_10_holder_rate=snapshot.top_10_holder_rate,
                    bundler_rate=snapshot.bundler_rate,
                    insider_rate=snapshot.insider_rate,
                    entrapment_ratio=snapshot.entrapment_ratio,
                )
            )
        )

    async def complete_security(
        self,
        job: Dict[str, Any],
        market: MarketSnapshot,
    ) -> None:
        event = job["event"]
        now_ms = int(time.time() * 1000)
        try:
            security = await asyncio.to_thread(
                self.goplus.token_security,
                event.chain,
                event.token_address,
            )
        except ProviderUnavailable:
            self.provider_unavailable(event, "goplus")
            self.finish_job(job, "GOPLUS_UNAVAILABLE", now_ms)
            return
        except Exception as exc:
            self.provider_failure(event, "goplus", exc)
            self.finish_job(job, "GOPLUS_ERROR", now_ms)
            return
        payload = security.payload
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="goplus",
            observed_at_ms=security.observed_at_ms,
            status="ok",
            payload=payload,
            summary={
                "field_count": len(payload),
                "has_honeypot": "is_honeypot" in payload,
                "has_sell_tax": "sell_tax" in payload,
                "has_holder_count": "holder_count" in payload,
            },
        )
        self.counters["goplus_ok"] += 1
        self.last_success["goplus"] = security.observed_at_ms
        decision = self.finalize_enrichment(
            event,
            job["features"],
            market,
            payload,
            security.observed_at_ms,
        )
        self.finish_job(job, "FINAL_" + decision.delivery.upper(), now_ms)

    def finalize_enrichment(
        self,
        event: RadarEvent,
        initial: FeatureSnapshot,
        market: MarketSnapshot,
        security_payload: Dict[str, Any],
        security_observed_at_ms: int,
        security_override: Optional[tuple] = None,
    ) -> Decision:
        security_status, sell_ok, creator_risk = (
            security_override
            if security_override is not None
            else _security_features(security_payload)
        )
        evaluated_at_ms = max(
            event.received_at_ms,
            security_observed_at_ms,
            market.observed_at_ms,
        )
        features = replace(
            initial,
            evaluated_at_ms=evaluated_at_ms,
            security_status=security_status,
            sell_simulation_ok=sell_ok,
            creator_risk=creator_risk,
            price_age_ms=(
                max(0, evaluated_at_ms - market.observed_at_ms)
                if market.price_usd is not None
                else None
            ),
            liquidity_usd=market.liquidity_usd,
            volume_24h_usd=market.volume_24h_usd,
            buy_transactions_24h=market.buy_transactions_24h,
            sell_transactions_24h=market.sell_transactions_24h,
            market_cap_usd=(
                market.market_cap_usd
                if market.market_cap_usd is not None
                else market.fdv_usd
            ),
            holder_count=market.holder_count,
            creator_inbound_count=market.creator_inbound_count,
            creator_outbound_count=market.creator_outbound_count,
            creator_retention_ratio=market.creator_retention_ratio,
            identity_count=market.identity_count,
            identity_market_cap_rank=market.identity_market_cap_rank,
            external_buy_transactions_5m=(
                market.external_buy_transactions_5m
            ),
        )
        decision = evaluate(event, features, self.config)
        store_decision(self.connection, decision)
        self.counters["final_decisions"] += 1
        self.counters["final_" + decision.delivery] += 1
        if decision.delivery == "strong":
            self.enqueue_strong(event, decision, features, evaluated_at_ms)
        elif self.sample_eligible(
            event,
            decision,
            features,
            market,
            security_payload,
        ):
            self.enqueue_sample(event, decision, features, market)
        return decision

    def market_ready_for_enrichment(
        self,
        event: RadarEvent,
        market: MarketSnapshot,
    ) -> bool:
        if market.pair_count < 1 or market.price_usd is None:
            return False
        if event.chain != "bsc" or event.launchpad != "four_meme":
            return True
        buys = market.buy_transactions_5m
        sells = market.sell_transactions_5m
        volume = market.volume_5m_usd
        return (
            buys is not None
            and sells is not None
            and volume is not None
            and buys >= self.config.sample_min_buys_5m
            and buys > sells
            and volume >= self.config.sample_min_volume_5m_usd
        )

    def enqueue_strong(
        self,
        event: RadarEvent,
        decision: Decision,
        features: FeatureSnapshot,
        now_ms: int,
    ) -> None:
        dedupe_key = "%s:%s:%s" % (
            decision.ruleset_version,
            event.chain,
            event.token_address,
        )
        if enqueue_telegram(
            self.connection,
            decision=decision,
            dedupe_key=dedupe_key,
            message_text=format_alert(event, decision, features),
            now_ms=now_ms,
        ):
            self.counters["telegram_enqueued"] += 1
        else:
            self.counters["telegram_deduplicated"] += 1

    def sample_eligible(
        self,
        event: RadarEvent,
        decision: Decision,
        features: FeatureSnapshot,
        market: MarketSnapshot,
        security_payload: Dict[str, Any],
    ) -> bool:
        if not self.settings.sample_enabled:
            return False
        if decision.risk_verdict != "review" or decision.delivery != "weak":
            return False
        if (
            decision.confidence_score < self.config.sample_confidence_min
            or decision.opportunity_score < self.config.sample_opportunity_min
            or market.pair_count < 1
            or market.price_usd is None
        ):
            return False
        buys = (
            features.external_buy_transactions_5m
            if features.external_buy_transactions_5m is not None
            else market.buy_transactions_5m
        )
        sells = market.sell_transactions_5m
        volume = market.volume_5m_usd
        if (
            buys is None
            or sells is None
            or volume is None
            or buys < self.config.sample_min_buys_5m
            or buys <= sells
            or volume < self.config.sample_min_volume_5m_usd
        ):
            return False
        if features.narrative_burst < 3 and features.cross_chain_count < 2:
            return False
        if (
            features.market_cap_usd is None
            or features.market_cap_usd <= MIN_MARKET_CAP_USD
            or features.holder_count is None
            or features.holder_count <= MIN_HOLDER_COUNT
        ):
            return False
        if (
            features.creator_outbound_count not in {0, None}
            or features.identity_count is None
            or features.identity_market_cap_rank != 1
        ):
            return False
        created_at_ms = event.token_created_at_ms or event.received_at_ms
        if decision.evaluated_at_ms - created_at_ms > self.config.sample_max_age_ms:
            return False
        dangerous = (
            "is_honeypot",
            "cannot_sell_all",
            "is_blacklisted",
            "is_mintable",
            "hidden_owner",
            "selfdestruct",
            "external_call",
            "transfer_pausable",
        )
        if any(_flag(security_payload, key) is True for key in dangerous):
            return False
        allowed_review = {
            "SECURITY_UNKNOWN",
            "SELL_SIMULATION_UNKNOWN",
            "CREATOR_RISK_UNKNOWN",
        }
        risk_reasons = {
            code
            for code in decision.reason_codes
            if not code.startswith(("CONFIDENCE_", "OPPORTUNITY_", "DELIVERY_"))
        }
        return bool(risk_reasons) and risk_reasons <= allowed_review

    def enqueue_sample(
        self,
        event: RadarEvent,
        decision: Decision,
        features: FeatureSnapshot,
        market: MarketSnapshot,
    ) -> None:
        status = enqueue_observation_sample(
            self.connection,
            decision=decision,
            dedupe_key="%s:%s:%s" % (
                self.config.sample_version,
                event.chain,
                event.token_address,
            ),
            message_text=format_observation_sample(
                event,
                decision,
                features,
                market,
            ),
            now_ms=decision.evaluated_at_ms,
            max_per_hour=self.settings.sample_max_per_hour,
        )
        self.counters["sample_" + status] += 1

    async def enrichment_loop(self) -> None:
        while not self.stop_event.is_set():
            now_ms = int(time.time() * 1000)
            self.last_success["enrichment_loop"] = now_ms
            self.counters["enrichment_polls"] += 1
            initial_chains = tuple(
                chain
                for chain, budget in self.initial_chain_budgets.items()
                if budget.available()
            )
            job = claim_due_enrichment(
                self.connection,
                now_ms=now_ms,
                initial_available=(
                    self.initial_probe_budget.available()
                    and self.gmgn_budget.available()
                ),
                initial_chains=initial_chains,
                size_recheck_available=(
                    self.size_recheck_budget.available()
                    and self.gmgn_budget.available()
                ),
                dex_recheck_available=self.dex_recheck_budget.available(),
                dex_available=self.dex_budget.available(),
                goplus_available=self.goplus_budget.available(),
            )
            if job is None:
                await self.wait_or_stop(1)
                continue
            self.counters["enrichment_claimed"] += 1
            try:
                if job["stage"] == "security_check":
                    await self.process_security_stage(job)
                else:
                    await self.process_market_stage(job)
            except Exception as exc:
                self.record_error("enrichment", exc)
                self.finish_job(
                    job,
                    "INTERNAL_ERROR",
                    int(time.time() * 1000),
                )

    async def telegram_loop(self) -> None:
        if self.telegram is None:
            return
        while not self.stop_event.is_set():
            if not self.telegram_budget.available():
                await self.wait_or_stop(30)
                continue
            now_ms = int(time.time() * 1000)
            item = claim_due_telegram(
                self.connection,
                now_ms=now_ms,
                max_attempts=self.settings.telegram_max_attempts,
            )
            if item is None:
                await self.wait_or_stop(1)
                continue
            if not self.telegram_budget.take():
                mark_telegram_retry(
                    self.connection,
                    item,
                    next_attempt_at_ms=now_ms + 30_000,
                    error_code="HOURLY_LIMIT",
                )
                continue
            try:
                result = await asyncio.to_thread(
                    self.telegram.send_message,
                    item["message_text"],
                )
                mark_telegram_sent(
                    self.connection,
                    item,
                    sent_at_ms=result.received_at_ms,
                )
                self.counters["telegram_sent"] += 1
                self.last_success["telegram"] = result.received_at_ms
            except Exception as exc:
                delay_seconds = min(3600, 30 * (2 ** (item["attempts"] - 1)))
                if isinstance(exc, HttpBoundaryError) and exc.status == 429:
                    delay_seconds = max(delay_seconds, 60)
                    self.telegram_budget.cooldown(delay_seconds)
                    self.counters["telegram_429"] += 1
                mark_telegram_retry(
                    self.connection,
                    item,
                    next_attempt_at_ms=int(time.time() * 1000)
                    + delay_seconds * 1000,
                    error_code=type(exc).__name__,
                )
                self.record_error("telegram", exc)
            await self.wait_or_stop(
                self.settings.telegram_min_interval_seconds
            )

    def health_payload(self) -> Dict:
        job_counts = enrichment_job_counts(self.connection)
        return {
            "schema_version": 1,
            "status": self.status,
            "updated_at_ms": int(time.time() * 1000),
            "started_at_ms": self.started_at_ms,
            "read_only": True,
            "no_signing": True,
            "no_broadcast": True,
            "ruleset_version": self.config.ruleset_version,
            "feature_version": self.config.feature_version,
            "unified_activation_ms": self.unified_activation_ms,
            "telegram_enabled": self.settings.telegram_enabled,
            "sample_enabled": self.settings.sample_enabled,
            "sample_max_per_hour": self.settings.sample_max_per_hour,
            "fomo_enabled": self.settings.fomo_enabled,
            "gmgn_enabled": self.settings.gmgn_enabled,
            "gmgn_discovery_enabled": (
                self.settings.gmgn_discovery_enabled
            ),
            "gmgn_discovery_chains": ["bsc", "base", "solana"],
            "bitquery_enabled": self.settings.bitquery_enabled,
            "bitquery_connected": self.bitquery_connected,
            "bitquery_token_slot": self.bitquery_slot(),
            "native_rpc": {
                "mode": self.settings.native_rpc_mode,
                "chains": {
                    chain: {
                        "connected": self.native_connected.get(chain, False),
                        "slot": self.native_slot(chain),
                        "primary_probe_successes": self.native_primary_probe_successes.get(
                            chain, 0
                        ),
                        "checkpoint_hold": self.native_checkpoint_hold.get(chain),
                    }
                    for chain in sorted(self.native_endpoints)
                },
            },
            "queue_depth": job_counts["pending"] + job_counts["processing"],
            "enrichment_jobs": job_counts,
            "counters": dict(sorted(self.counters.items())),
            "last_success": dict(sorted(self.last_success.items())),
            "last_error": dict(sorted(self.last_error.items())),
            "budgets": {
                "goplus": self.goplus_budget.snapshot(),
                "dexscreener": self.dex_budget.snapshot(),
                "gmgn": self.gmgn_budget.snapshot(),
                "gmgn_discovery": self.gmgn_discovery_budget.snapshot(),
                "initial_candidates": self.initial_probe_budget.snapshot(),
                "size_rechecks": self.size_recheck_budget.snapshot(),
                "dex_rechecks": self.dex_recheck_budget.snapshot(),
                "initial_by_chain": {
                    chain: budget.snapshot()
                    for chain, budget in sorted(self.initial_chain_budgets.items())
                },
                "telegram": self.telegram_budget.snapshot(),
            },
            "telegram_outbox": telegram_outbox_counts(self.connection),
        }

    async def health_loop(self) -> None:
        while not self.stop_event.is_set():
            write_health(self.settings.health_path, self.health_payload())
            await self.wait_or_stop(15)

    async def run(self, run_seconds: int = 0) -> None:
        self.status = "running"
        write_health(self.settings.health_path, self.health_payload())
        _log(
            "MEME_RADAR_START",
            read_only=True,
            no_signing=True,
            no_broadcast=True,
        )
        source_tasks = [self.tracked_task("clanker", self.clanker_loop())]
        if self.settings.fomo_enabled:
            source_tasks.append(self.tracked_task("fomo", self.fomo_loop()))
        if self.settings.gmgn_discovery_enabled:
            source_tasks.append(
                self.tracked_task(
                    "gmgn_discovery",
                    self.gmgn_discovery_loop(),
                )
            )
        if self.settings.bitquery_enabled:
            source_tasks.append(
                self.tracked_task("bitquery", self.bitquery_loop())
            )
        if self.settings.native_rpc_mode != "off":
            source_tasks.extend(
                self.tracked_task(
                    "native_" + chain,
                    self.native_rpc_loop(chain),
                )
                for chain in sorted(self.native_endpoints)
            )
            source_tasks.extend(
                self.tracked_task(
                    "native_primary_probe_" + chain,
                    self.native_primary_probe_loop(chain),
                )
                for chain in sorted(self.native_endpoints)
                if len(self.native_endpoints[chain]) > 1
            )
        enrichment_task = self.tracked_task(
            "enrichment",
            self.enrichment_loop(),
        )
        telegram_task = (
            self.tracked_task("telegram", self.telegram_loop())
            if self.settings.telegram_enabled
            else None
        )
        health_task = self.tracked_task("health", self.health_loop())
        timed_task = None
        if run_seconds:
            async def timed_stop():
                await asyncio.sleep(run_seconds)
                self.stop_event.set()
            timed_task = asyncio.create_task(timed_stop())
        await self.stop_event.wait()
        for task in source_tasks:
            task.cancel()
        await asyncio.gather(*source_tasks, return_exceptions=True)
        try:
            await asyncio.wait_for(enrichment_task, timeout=15)
        except asyncio.TimeoutError:
            self.counters["shutdown_enrichment_timeout"] += 1
            enrichment_task.cancel()
        health_task.cancel()
        if telegram_task is not None:
            telegram_task.cancel()
        if timed_task is not None:
            timed_task.cancel()
        await asyncio.gather(
            enrichment_task,
            health_task,
            *([telegram_task] if telegram_task is not None else []),
            *([timed_task] if timed_task is not None else []),
            return_exceptions=True,
        )
        self.bitquery_connected = False
        self.status = "stopped"
        write_health(self.settings.health_path, self.health_payload())
        _log(
            "MEME_RADAR_STOP",
            counters=dict(sorted(self.counters.items())),
            no_broadcast=True,
        )
        if self.fatal_task_error is not None:
            raise RuntimeError("background task failed") from self.fatal_task_error


def arguments():
    parser = argparse.ArgumentParser(description="Meme Radar shadow daemon")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--health-path", type=Path)
    parser.add_argument("--run-seconds", type=int, default=0)
    return parser.parse_args()


async def async_main(args) -> int:
    settings = RuntimeSettings.from_env_file(args.env_file)
    if args.db_path:
        settings = replace(
            settings,
            db_path=args.db_path,
            health_path=args.health_path or settings.health_path,
        )
        settings.validate()
    elif args.health_path:
        raise ValueError("--health-path requires --db-path")
    if not 0 <= args.run_seconds <= 86_400:
        raise ValueError("run-seconds outside 0..86400")
    credentials = load_credentials(args.env_file)
    daemon = RadarDaemon(settings, credentials)
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, daemon.stop_event.set)
        except NotImplementedError:
            pass
    try:
        await daemon.run(args.run_seconds)
    finally:
        daemon.close()
    return 0


def main() -> int:
    args = arguments()
    try:
        return asyncio.run(async_main(args))
    except Exception as exc:
        _log(
            "MEME_RADAR_FATAL",
            error_type=type(exc).__name__,
            no_signing=True,
            no_broadcast=True,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
