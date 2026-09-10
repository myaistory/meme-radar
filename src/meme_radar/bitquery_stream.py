from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import websockets

from .launchpads import (
    FOURMEME_BSC_PROXY,
    PONS_V2_FACTORY,
    PONS_V2_LAUNCH_SELECTORS,
    PONS_V2_ROUTER,
)


BITQUERY_WS = "wss://streaming.bitquery.io/graphql"

FOURMEME_SUBSCRIPTION = """
subscription {
  EVM(network: bsc) {
    Events(
      where: {
        LogHeader: {Address: {is: "%s"}}
        Log: {Signature: {Name: {is: "TokenCreate"}}}
      }
    ) {
      Block { Time }
      Transaction { Hash From }
      Arguments {
        Name
        Value {
          ... on EVM_ABI_String_Value_Arg { string }
          ... on EVM_ABI_Address_Value_Arg { address }
          ... on EVM_ABI_BigInt_Value_Arg { bigInteger }
        }
      }
    }
  }
}
""" % FOURMEME_BSC_PROXY

PONS_CALLS_SUBSCRIPTION = """
subscription {
  EVM(network: robinhood) {
    Calls(
      where: {
        Call: {
          To: {in: ["%s", "%s"]}
          Input: {startsWith: %s}
          Success: true
        }
      }
    ) {
      Block { Time }
      Transaction { Hash From }
      Call { To Value Input Output }
    }
  }
}
""" % (
    PONS_V2_FACTORY,
    PONS_V2_ROUTER,
    json.dumps(sorted(PONS_V2_LAUNCH_SELECTORS)),
)

PONS_EVENTS_SUBSCRIPTION = """
subscription {
  EVM(network: robinhood) {
    Events(
      where: {
        LogHeader: {Address: {is: "%s"}}
        Log: {Signature: {Name: {is: "TokenLaunched"}}}
      }
    ) {
      Block { Time }
      Transaction { Hash From }
      Arguments {
        Name
        Value {
          ... on EVM_ABI_String_Value_Arg { string }
          ... on EVM_ABI_Address_Value_Arg { address }
          ... on EVM_ABI_BigInt_Value_Arg { bigInteger }
        }
      }
    }
  }
}
""" % PONS_V2_FACTORY

DEFAULT_SUBSCRIPTIONS = {
    "fourmeme": FOURMEME_SUBSCRIPTION,
    "pons_events": PONS_EVENTS_SUBSCRIPTION,
}


def _without_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate json key")
        result[key] = value
    return result


@dataclass(frozen=True)
class StreamRecord:
    subscription_id: str
    received_at_ms: int
    data: Dict


@dataclass(frozen=True)
class StreamMetrics:
    connected: bool
    acknowledged: bool
    records: Tuple[StreamRecord, ...]
    protocol_errors: Tuple[str, ...]
    pings: int


def decode_protocol_message(
    raw: str,
    subscription_ids: Iterable[str],
) -> Tuple[str, Optional[StreamRecord], Optional[str]]:
    if len(raw.encode("utf-8")) > 256 * 1024:
        return "error", None, "MESSAGE_TOO_LARGE"
    try:
        message = json.loads(raw, object_pairs_hook=_without_duplicate_keys)
    except (json.JSONDecodeError, ValueError):
        return "error", None, "INVALID_JSON"
    if not isinstance(message, dict):
        return "error", None, "INVALID_MESSAGE"
    kind = message.get("type")
    if kind in {"ping", "pong", "connection_ack", "complete"}:
        return str(kind), None, None
    if kind == "error":
        sub_id = str(message.get("id", ""))
        return "error", None, "SUBSCRIPTION_ERROR_%s" % sub_id
    if kind != "next":
        return "error", None, "UNKNOWN_MESSAGE_TYPE"
    sub_id = str(message.get("id", ""))
    if sub_id not in set(subscription_ids):
        return "error", None, "UNKNOWN_SUBSCRIPTION"
    payload = message.get("payload")
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return "error", None, "MISSING_DATA"
    return (
        "next",
        StreamRecord(
            subscription_id=sub_id,
            received_at_ms=int(time.time() * 1000),
            data=data,
        ),
        None,
    )


async def collect_records(
    *,
    token: str,
    duration_seconds: float,
    subscriptions: Dict[str, str] = DEFAULT_SUBSCRIPTIONS,
    max_records: int = 1000,
) -> StreamMetrics:
    if not token or len(token) > 4096 or "\r" in token or "\n" in token:
        raise ValueError("invalid Bitquery token")
    if not 1 <= duration_seconds <= 300:
        raise ValueError("duration must be 1..300 seconds")
    if not subscriptions or len(subscriptions) > 4:
        raise ValueError("invalid subscription count")
    if not 1 <= max_records <= 10_000:
        raise ValueError("invalid record cap")

    url = BITQUERY_WS + "?token=" + quote(token, safe="")
    records: List[StreamRecord] = []
    errors: List[str] = []
    pings = 0
    acknowledged = False
    async with websockets.connect(
        url,
        subprotocols=["graphql-transport-ws"],
        open_timeout=10,
        close_timeout=3,
        ping_interval=20,
        max_size=256 * 1024,
        max_queue=32,
    ) as websocket:
        await websocket.send(
            json.dumps({"type": "connection_init", "payload": {}})
        )
        raw_ack = await asyncio.wait_for(websocket.recv(), timeout=10)
        ack_kind, _, ack_error = decode_protocol_message(
            raw_ack, subscriptions.keys()
        )
        if ack_kind != "connection_ack":
            raise RuntimeError(ack_error or "Bitquery connection not acknowledged")
        acknowledged = True
        for sub_id, query in subscriptions.items():
            await websocket.send(
                json.dumps(
                    {
                        "id": sub_id,
                        "type": "subscribe",
                        "payload": {"query": query},
                    }
                )
            )

        deadline = time.monotonic() + duration_seconds
        while time.monotonic() < deadline and len(records) < max_records:
            remaining = deadline - time.monotonic()
            try:
                raw = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=min(5.0, max(0.05, remaining)),
                )
            except asyncio.TimeoutError:
                continue
            kind, record, error = decode_protocol_message(
                raw, subscriptions.keys()
            )
            if kind == "ping":
                pings += 1
                await websocket.send(json.dumps({"type": "pong"}))
            elif kind == "next" and record is not None:
                records.append(record)
            elif kind == "error" and error is not None:
                errors.append(error)
        for sub_id in subscriptions:
            await websocket.send(
                json.dumps({"id": sub_id, "type": "complete"})
            )

    return StreamMetrics(
        connected=True,
        acknowledged=acknowledged,
        records=tuple(records),
        protocol_errors=tuple(errors),
        pings=pings,
    )


async def stream_records_once(
    *,
    token: str,
    stop_event: asyncio.Event,
    subscriptions: Dict[str, str] = DEFAULT_SUBSCRIPTIONS,
):
    if not token or len(token) > 4096 or "\r" in token or "\n" in token:
        raise ValueError("invalid Bitquery token")
    if not subscriptions or len(subscriptions) > 4:
        raise ValueError("invalid subscription count")
    url = BITQUERY_WS + "?token=" + quote(token, safe="")
    async with websockets.connect(
        url,
        subprotocols=["graphql-transport-ws"],
        open_timeout=10,
        close_timeout=3,
        ping_interval=20,
        max_size=256 * 1024,
        max_queue=32,
    ) as websocket:
        await websocket.send(
            json.dumps({"type": "connection_init", "payload": {}})
        )
        raw_ack = await asyncio.wait_for(websocket.recv(), timeout=10)
        ack_kind, _, ack_error = decode_protocol_message(
            raw_ack, subscriptions.keys()
        )
        if ack_kind != "connection_ack":
            raise RuntimeError(ack_error or "Bitquery connection not acknowledged")
        for sub_id, query in subscriptions.items():
            await websocket.send(
                json.dumps(
                    {
                        "id": sub_id,
                        "type": "subscribe",
                        "payload": {"query": query},
                    }
                )
            )
        try:
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                kind, record, error = decode_protocol_message(
                    raw, subscriptions.keys()
                )
                if kind == "ping":
                    await websocket.send(json.dumps({"type": "pong"}))
                elif kind == "next" and record is not None:
                    yield record
                elif kind == "error":
                    raise RuntimeError(error or "Bitquery protocol error")
        finally:
            for sub_id in subscriptions:
                try:
                    await websocket.send(
                        json.dumps({"id": sub_id, "type": "complete"})
                    )
                except Exception:
                    break
