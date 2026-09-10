from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Tuple

from .evm_rpc import PONS_TOKEN_LAUNCHED_TOPIC
from .launchpads import PONS_V2_FACTORY, PONS_V2_ROUTER


PONS_CURVE_BUY_TOPIC = (
    "0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455"
)
PONS_CURVE_SELL_TOPIC = (
    "0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df"
)
PONS_CURVE_COMPLETED_TOPIC = (
    "0xf8d37a90738ae063b8b8058b66f5880cf3cf7ab0c5d4fa78219696591dfbfb67"
)
PONS_POOL_GRADUATED_TOPIC = (
    "0x0a44ef75df69c534f43cd6c1aa3ef8983065fe5fe79ef9e79f6494e6f258c259"
)

PONS_FACTORY_TOPICS = (PONS_TOKEN_LAUNCHED_TOPIC, PONS_POOL_GRADUATED_TOPIC)
PONS_CURVE_TOPICS = (
    PONS_CURVE_BUY_TOPIC,
    PONS_CURVE_SELL_TOPIC,
    PONS_CURVE_COMPLETED_TOPIC,
)

_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_WORD = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass(frozen=True)
class PonsLaunch:
    token_address: str
    curve_address: str
    creator_address: str
    pair_token_address: str
    launch_config_id: int
    graduation_threshold_raw: int
    block_number: int
    block_hash: str
    transaction_hash: str
    log_index: int
    block_timestamp_ms: int
    observed_at_ms: int
    removed: bool = False


@dataclass(frozen=True)
class PonsCurveTrade:
    token_address: str
    curve_address: str
    event_kind: str
    trader_address: str
    recipient_address: str
    quote_amount_raw: int
    token_amount_raw: int
    fee_raw: int
    tax_raw: int
    block_number: int
    block_hash: str
    transaction_hash: str
    log_index: int
    block_timestamp_ms: int
    observed_at_ms: int
    is_creator_initial: bool
    removed: bool = False


@dataclass(frozen=True)
class PonsLifecycle:
    token_address: str
    curve_address: str
    event_kind: str
    quote_amount_raw: int
    token_amount_raw: int
    block_number: int
    block_hash: str
    transaction_hash: str
    log_index: int
    block_timestamp_ms: int
    observed_at_ms: int
    removed: bool = False


def _hex_int(value: Any, label: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError("invalid " + label)
    try:
        number = int(value, 16)
    except ValueError as exc:
        raise ValueError("invalid " + label) from exc
    if number < 0:
        raise ValueError("invalid " + label)
    return number


def _address(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ADDRESS.fullmatch(value):
        raise ValueError("invalid " + label)
    return value.lower()


def _hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise ValueError("invalid " + label)
    return value.lower()


def _words(data: Any, minimum: int) -> Tuple[str, ...]:
    if not isinstance(data, str) or not data.startswith("0x"):
        raise ValueError("invalid log data")
    raw = data[2:]
    if len(raw) % 64 or len(raw) < minimum * 64:
        raise ValueError("short log data")
    words = tuple(raw[index : index + 64] for index in range(0, len(raw), 64))
    if any(not _WORD.fullmatch(word) for word in words):
        raise ValueError("invalid log word")
    return words


def _topics(log: Mapping[str, Any], minimum: int) -> Tuple[str, ...]:
    values = log.get("topics")
    if not isinstance(values, list) or len(values) < minimum:
        raise ValueError("invalid log topics")
    topics = tuple(_hash(value, "topic") for value in values)
    return topics


def _identity(log: Mapping[str, Any]) -> Tuple[int, str, str, int, bool]:
    return (
        _hex_int(log.get("blockNumber"), "block number"),
        _hash(log.get("blockHash"), "block hash"),
        _hash(log.get("transactionHash"), "transaction hash"),
        _hex_int(log.get("logIndex"), "log index"),
        log.get("removed") is True,
    )


def _topic_address(topic: str, label: str) -> str:
    return _address("0x" + topic[-40:], label)


def _word_address(word: str, label: str) -> str:
    return _address("0x" + word[-40:], label)


def parse_pons_launch(
    log: Mapping[str, Any],
    *,
    block_timestamp_ms: int,
    observed_at_ms: int,
) -> PonsLaunch:
    if _address(log.get("address"), "factory") != PONS_V2_FACTORY:
        raise ValueError("launch factory mismatch")
    topics = _topics(log, 4)
    if topics[0] != PONS_TOKEN_LAUNCHED_TOPIC:
        raise ValueError("launch topic mismatch")
    words = _words(log.get("data"), 3)
    block, block_hash, tx_hash, log_index, removed = _identity(log)
    return PonsLaunch(
        token_address=_topic_address(topics[1], "token"),
        curve_address=_topic_address(topics[2], "curve"),
        creator_address=_topic_address(topics[3], "creator"),
        pair_token_address=_word_address(words[0], "pair token"),
        launch_config_id=int(words[1], 16),
        graduation_threshold_raw=int(words[2], 16),
        block_number=block,
        block_hash=block_hash,
        transaction_hash=tx_hash,
        log_index=log_index,
        block_timestamp_ms=block_timestamp_ms,
        observed_at_ms=observed_at_ms,
        removed=removed,
    )


def parse_pons_trade(
    log: Mapping[str, Any],
    launch: PonsLaunch,
    *,
    block_timestamp_ms: int,
    observed_at_ms: int,
) -> PonsCurveTrade:
    curve = _address(log.get("address"), "curve")
    if curve != launch.curve_address or launch.removed:
        raise ValueError("unregistered curve")
    topics = _topics(log, 3)
    if topics[0] not in {PONS_CURVE_BUY_TOPIC, PONS_CURVE_SELL_TOPIC}:
        raise ValueError("trade topic mismatch")
    values = tuple(int(word, 16) for word in _words(log.get("data"), 4))
    block, block_hash, tx_hash, log_index, removed = _identity(log)
    is_buy = topics[0] == PONS_CURVE_BUY_TOPIC
    quote_amount, token_amount = (values[0], values[1]) if is_buy else (values[1], values[0])
    trader = _topic_address(topics[1], "trader")
    recipient = _topic_address(topics[2], "recipient")
    return PonsCurveTrade(
        token_address=launch.token_address,
        curve_address=curve,
        event_kind="buy" if is_buy else "sell",
        trader_address=trader,
        recipient_address=recipient,
        quote_amount_raw=quote_amount,
        token_amount_raw=token_amount,
        fee_raw=values[2],
        tax_raw=values[3],
        block_number=block,
        block_hash=block_hash,
        transaction_hash=tx_hash,
        log_index=log_index,
        block_timestamp_ms=block_timestamp_ms,
        observed_at_ms=observed_at_ms,
        is_creator_initial=(
            is_buy
            and trader == PONS_V2_ROUTER
            and recipient == launch.creator_address
            and tx_hash == launch.transaction_hash
        ),
        removed=removed,
    )


def parse_pons_lifecycle(
    log: Mapping[str, Any],
    launches_by_curve: Mapping[str, PonsLaunch],
    launches_by_token: Mapping[str, PonsLaunch],
    *,
    block_timestamp_ms: int,
    observed_at_ms: int,
) -> PonsLifecycle:
    address = _address(log.get("address"), "emitter")
    topics = _topics(log, 1)
    block, block_hash, tx_hash, log_index, removed = _identity(log)
    if topics[0] == PONS_CURVE_COMPLETED_TOPIC:
        launch = launches_by_curve.get(address)
        if launch is None or launch.removed:
            raise ValueError("unregistered completed curve")
        words = _words(log.get("data"), 3)
        quote_amount, token_amount = int(words[1], 16), int(words[2], 16)
        kind = "curve_completed"
    elif topics[0] == PONS_POOL_GRADUATED_TOPIC:
        if address != PONS_V2_FACTORY or len(topics) < 2:
            raise ValueError("graduation factory mismatch")
        launch = launches_by_token.get(_topic_address(topics[1], "token"))
        if launch is None or launch.removed:
            raise ValueError("unregistered graduated token")
        words = _words(log.get("data"), 3)
        quote_amount, token_amount = int(words[2], 16), int(words[1], 16)
        kind = "pool_graduated"
    else:
        raise ValueError("lifecycle topic mismatch")
    return PonsLifecycle(
        token_address=launch.token_address,
        curve_address=launch.curve_address,
        event_kind=kind,
        quote_amount_raw=quote_amount,
        token_amount_raw=token_amount,
        block_number=block,
        block_hash=block_hash,
        transaction_hash=tx_hash,
        log_index=log_index,
        block_timestamp_ms=block_timestamp_ms,
        observed_at_ms=observed_at_ms,
        removed=removed,
    )
