from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable

from .pons_curve import PonsCurveTrade, PonsLaunch, PonsLifecycle


SHADOW_SCHEMA_VERSION = 1

SHADOW_SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS pons_curve_launches (
    token_address TEXT PRIMARY KEY,
    curve_address TEXT NOT NULL UNIQUE,
    creator_address TEXT NOT NULL,
    pair_token_address TEXT NOT NULL,
    launch_config_id TEXT NOT NULL,
    graduation_threshold_raw TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    removed INTEGER NOT NULL CHECK (removed IN (0,1)),
    UNIQUE (transaction_hash, log_index)
);
CREATE TABLE IF NOT EXISTS pons_curve_trades (
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    token_address TEXT NOT NULL REFERENCES pons_curve_launches(token_address),
    curve_address TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (event_kind IN ('buy','sell')),
    trader_address TEXT NOT NULL,
    recipient_address TEXT NOT NULL,
    quote_amount_raw TEXT NOT NULL,
    token_amount_raw TEXT NOT NULL,
    fee_raw TEXT NOT NULL,
    tax_raw TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    is_creator_initial INTEGER NOT NULL CHECK (is_creator_initial IN (0,1)),
    removed INTEGER NOT NULL CHECK (removed IN (0,1)),
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE TABLE IF NOT EXISTS pons_curve_lifecycle (
    transaction_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    token_address TEXT NOT NULL REFERENCES pons_curve_launches(token_address),
    curve_address TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (
      event_kind IN ('curve_completed','pool_graduated')
    ),
    quote_amount_raw TEXT NOT NULL,
    token_amount_raw TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    block_timestamp_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    removed INTEGER NOT NULL CHECK (removed IN (0,1)),
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE TABLE IF NOT EXISTS pons_curve_shadow_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pons_curve_trades_token_time
  ON pons_curve_trades(token_address, block_timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_pons_curve_trades_curve_time
  ON pons_curve_trades(curve_address, block_timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_pons_curve_lifecycle_token
  ON pons_curve_lifecycle(token_address, block_timestamp_ms);
"""


def connect_shadow(path: Path) -> sqlite3.Connection:
    existed = path.exists()
    connection = sqlite3.connect(str(path), timeout=10)
    if not existed:
        path.chmod(0o600)
    connection.row_factory = sqlite3.Row
    mode = str(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0])
    if mode.lower() != "wal":
        connection.close()
        raise RuntimeError("SQLite WAL unavailable")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def initialize_shadow(connection: sqlite3.Connection) -> None:
    current = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current not in {0, SHADOW_SCHEMA_VERSION}:
        raise RuntimeError("unsupported Pons shadow schema")
    with connection:
        connection.executescript(SHADOW_SCHEMA)
        connection.execute("PRAGMA user_version=%d" % SHADOW_SCHEMA_VERSION)


def _launch_identity(launch: PonsLaunch) -> tuple:
    return (
        launch.curve_address,
        launch.creator_address,
        launch.pair_token_address,
        str(launch.launch_config_id),
        str(launch.graduation_threshold_raw),
        launch.block_number,
        launch.block_hash,
        launch.transaction_hash,
        launch.log_index,
        launch.block_timestamp_ms,
    )


def store_launch(
    connection: sqlite3.Connection,
    launch: PonsLaunch,
    *,
    allow_timestamp_correction: bool = False,
) -> bool:
    existing = connection.execute(
        """
        SELECT curve_address,creator_address,pair_token_address,launch_config_id,
               graduation_threshold_raw,block_number,block_hash,transaction_hash,
               log_index,block_timestamp_ms
        FROM pons_curve_launches WHERE token_address=?
        """,
        (launch.token_address,),
    ).fetchone()
    incoming_identity = _launch_identity(launch)
    timestamp_correction = False
    if existing is not None and tuple(existing) != incoming_identity:
        timestamp_correction = (
            allow_timestamp_correction
            and tuple(existing)[:-1] == incoming_identity[:-1]
        )
        if not timestamp_correction:
            raise RuntimeError("conflicting Pons launch identity")
    with connection:
        if existing is not None:
            connection.execute(
                """
                UPDATE pons_curve_launches
                SET block_timestamp_ms=CASE WHEN ? THEN ? ELSE block_timestamp_ms END,
                    removed=CASE WHEN ?>=observed_at_ms THEN ? ELSE removed END,
                    observed_at_ms=max(observed_at_ms,?)
                WHERE token_address=?
                """,
                (
                    int(timestamp_correction),
                    launch.block_timestamp_ms,
                    launch.observed_at_ms,
                    int(launch.removed),
                    launch.observed_at_ms,
                    launch.token_address,
                ),
            )
            return False
        connection.execute(
            """
            INSERT INTO pons_curve_launches
              (token_address,curve_address,creator_address,pair_token_address,
               launch_config_id,graduation_threshold_raw,block_number,block_hash,
               transaction_hash,log_index,block_timestamp_ms,observed_at_ms,removed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                launch.token_address,
                launch.curve_address,
                launch.creator_address,
                launch.pair_token_address,
                str(launch.launch_config_id),
                str(launch.graduation_threshold_raw),
                launch.block_number,
                launch.block_hash,
                launch.transaction_hash,
                launch.log_index,
                launch.block_timestamp_ms,
                launch.observed_at_ms,
                int(launch.removed),
            ),
        )
    return True


def _store_event(
    connection: sqlite3.Connection,
    *,
    table: str,
    identity: tuple,
    values: tuple,
    immutable: tuple,
    observed_at_ms: int,
    removed: bool,
) -> bool:
    columns = {
        "pons_curve_trades": (
            "token_address,curve_address,event_kind,trader_address,recipient_address,"
            "quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,"
            "block_timestamp_ms,is_creator_initial"
        ),
        "pons_curve_lifecycle": (
            "token_address,curve_address,event_kind,quote_amount_raw,token_amount_raw,"
            "block_number,block_hash,block_timestamp_ms"
        ),
    }[table]
    existing = connection.execute(
        "SELECT " + columns + " FROM " + table
        + " WHERE transaction_hash=? AND log_index=?",
        identity,
    ).fetchone()
    if existing is not None and tuple(existing) != immutable:
        raise RuntimeError("conflicting Pons shadow event identity")
    with connection:
        if existing is not None:
            connection.execute(
                "UPDATE " + table
                + " SET removed=CASE WHEN ?>=observed_at_ms THEN ? ELSE removed END,"
                + " observed_at_ms=max(observed_at_ms,?)"
                + " WHERE transaction_hash=? AND log_index=?",
                (
                    observed_at_ms,
                    int(removed),
                    observed_at_ms,
                    *identity,
                ),
            )
            return False
        placeholders = ",".join("?" for _ in values)
        connection.execute(
            "INSERT INTO " + table + " VALUES (" + placeholders + ")",
            values,
        )
    return True


def store_trade(connection: sqlite3.Connection, trade: PonsCurveTrade) -> bool:
    identity = (trade.transaction_hash, trade.log_index)
    immutable = (
        trade.token_address,
        trade.curve_address,
        trade.event_kind,
        trade.trader_address,
        trade.recipient_address,
        str(trade.quote_amount_raw),
        str(trade.token_amount_raw),
        str(trade.fee_raw),
        str(trade.tax_raw),
        trade.block_number,
        trade.block_hash,
        trade.block_timestamp_ms,
        int(trade.is_creator_initial),
    )
    values = (
        *identity,
        *immutable[:9],
        immutable[9],
        immutable[10],
        immutable[11],
        trade.observed_at_ms,
        immutable[12],
        int(trade.removed),
    )
    return _store_event(
        connection,
        table="pons_curve_trades",
        identity=identity,
        values=values,
        immutable=immutable,
        observed_at_ms=trade.observed_at_ms,
        removed=trade.removed,
    )


def store_lifecycle(connection: sqlite3.Connection, event: PonsLifecycle) -> bool:
    identity = (event.transaction_hash, event.log_index)
    immutable = (
        event.token_address,
        event.curve_address,
        event.event_kind,
        str(event.quote_amount_raw),
        str(event.token_amount_raw),
        event.block_number,
        event.block_hash,
        event.block_timestamp_ms,
    )
    values = (
        *identity,
        *immutable,
        event.observed_at_ms,
        int(event.removed),
    )
    return _store_event(
        connection,
        table="pons_curve_lifecycle",
        identity=identity,
        values=values,
        immutable=immutable,
        observed_at_ms=event.observed_at_ms,
        removed=event.removed,
    )


def load_launches(
    connection: sqlite3.Connection,
    *,
    min_block_timestamp_ms: int | None = None,
) -> Iterable[PonsLaunch]:
    query = "SELECT * FROM pons_curve_launches"
    parameters = ()
    if min_block_timestamp_ms is not None:
        query += " WHERE removed=0 AND block_timestamp_ms>=?"
        parameters = (min_block_timestamp_ms,)
    query += " ORDER BY block_number,log_index"
    rows = connection.execute(query, parameters)
    for row in rows:
        yield PonsLaunch(
            token_address=row["token_address"],
            curve_address=row["curve_address"],
            creator_address=row["creator_address"],
            pair_token_address=row["pair_token_address"],
            launch_config_id=int(row["launch_config_id"]),
            graduation_threshold_raw=int(row["graduation_threshold_raw"]),
            block_number=int(row["block_number"]),
            block_hash=row["block_hash"],
            transaction_hash=row["transaction_hash"],
            log_index=int(row["log_index"]),
            block_timestamp_ms=int(row["block_timestamp_ms"]),
            observed_at_ms=int(row["observed_at_ms"]),
            removed=bool(row["removed"]),
        )


def get_state(connection: sqlite3.Connection, key: str, default=None):
    row = connection.execute(
        "SELECT value_json FROM pons_curve_shadow_state WHERE key=?", (key,)
    ).fetchone()
    return default if row is None else json.loads(row["value_json"])


def set_state(connection: sqlite3.Connection, key: str, value, now_ms: int) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
    with connection:
        connection.execute(
            """
            INSERT INTO pons_curve_shadow_state(key,value_json,updated_at_ms)
            VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET
              value_json=excluded.value_json,updated_at_ms=excluded.updated_at_ms
            """,
            (key, encoded, now_ms),
        )


def shadow_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    queries = {
        "launches": "SELECT count(*) FROM pons_curve_launches WHERE removed=0",
        "buys": "SELECT count(*) FROM pons_curve_trades WHERE removed=0 AND event_kind='buy'",
        "sells": "SELECT count(*) FROM pons_curve_trades WHERE removed=0 AND event_kind='sell'",
        "creator_initial_buys": "SELECT count(*) FROM pons_curve_trades WHERE removed=0 AND is_creator_initial=1",
        "creator_buys": """SELECT count(*) FROM pons_curve_trades t
            JOIN pons_curve_launches l USING(token_address)
            WHERE t.removed=0 AND t.event_kind='buy'
              AND (t.is_creator_initial=1 OR t.trader_address=l.creator_address
                OR t.recipient_address=l.creator_address)""",
        "external_buys": """SELECT count(*) FROM pons_curve_trades t
            JOIN pons_curve_launches l USING(token_address)
            WHERE t.removed=0 AND t.event_kind='buy' AND t.is_creator_initial=0
              AND t.trader_address<>l.creator_address
              AND t.recipient_address<>l.creator_address""",
        "curve_completed": "SELECT count(*) FROM pons_curve_lifecycle WHERE removed=0 AND event_kind='curve_completed'",
        "pool_graduated": "SELECT count(*) FROM pons_curve_lifecycle WHERE removed=0 AND event_kind='pool_graduated'",
    }
    return {
        key: int(connection.execute(sql).fetchone()[0])
        for key, sql in queries.items()
    }
