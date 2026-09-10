from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


MIN_MARKET_CAP_USD = 100_000.0
MIN_HOLDER_COUNT = 100
FULL_CREATOR_RETENTION_RATIO = 0.999
MAX_GMGN_RISK_RATIO = 0.30
MAX_GMGN_TOP_10_RATE = 0.50
MIN_GMGN_DISCOVERY_LIQUIDITY_USD = 10_000.0
MIN_GMGN_DISCOVERY_VOLUME_5M_USD = 1_000.0
MIN_GMGN_DISCOVERY_BUYS_5M = 10
MIN_GMGN_DISCOVERY_SMART_MONEY = 3


@dataclass(frozen=True)
class UnifiedSafetyEvidence:
    market_cap_usd: Optional[float]
    creator_inbound_count: Optional[int]
    creator_outbound_count: Optional[int]
    creator_retention_ratio: Optional[float]
    creator_balance_present: Optional[bool] = False
    identity_count: int = 1
    identity_market_cap_rank: Optional[int] = 1


@dataclass(frozen=True)
class GmgnSafetyEvidence:
    market_cap_usd: Optional[float]
    creator_token_status: str
    is_honeypot: Optional[bool]
    is_open_source: Optional[bool]
    owner_renounced: Optional[bool]
    is_wash_trading: Optional[bool]
    rug_ratio: Optional[float]
    top_10_holder_rate: Optional[float]
    bundler_rate: Optional[float]
    insider_rate: Optional[float]
    entrapment_ratio: Optional[float]


@dataclass(frozen=True)
class GmgnDiscoveryEvidence:
    chain: str
    market_cap_usd: Optional[float]
    holder_count: Optional[int]
    liquidity_usd: Optional[float]
    volume_5m_usd: Optional[float]
    swaps_5m: Optional[int]
    buys_5m: Optional[int]
    sells_5m: Optional[int]
    smart_degen_count: Optional[int]
    creator_close: Optional[bool]
    is_honeypot: Optional[bool]
    is_open_source: Optional[bool]
    owner_renounced: Optional[bool]
    renounced_mint: Optional[bool]
    renounced_freeze: Optional[bool]
    is_wash_trading: Optional[bool]
    rug_ratio: Optional[float]
    top_10_holder_rate: Optional[float]
    bundler_rate: Optional[float]
    insider_rate: Optional[float]
    entrapment_ratio: Optional[float]
    dev_team_hold_rate: Optional[float]


def market_size_reasons(
    market_cap_usd: Optional[float],
    holder_count: Optional[int],
) -> Tuple[str, ...]:
    reasons = []
    if market_cap_usd is None:
        reasons.append("market_cap_unknown")
    elif market_cap_usd <= MIN_MARKET_CAP_USD:
        reasons.append("market_cap_not_above_min")
    if holder_count is None:
        reasons.append("holder_count_unknown")
    elif holder_count <= MIN_HOLDER_COUNT:
        reasons.append("holder_count_not_above_min")
    return tuple(reasons)


def any_buy_sell_ratio_at_least_one(*pairs) -> bool:
    return any(
        buys is not None
        and sells is not None
        and buys / max(1, sells) >= 1.0
        for buys, sells in pairs
    )


def unified_safety_reasons(
    evidence: UnifiedSafetyEvidence,
) -> Tuple[str, ...]:
    reasons = []
    market_cap = evidence.market_cap_usd
    if market_cap is None:
        reasons.append("market_cap_unknown")
    elif market_cap <= MIN_MARKET_CAP_USD:
        reasons.append("market_cap_not_above_min")
    inbound = evidence.creator_inbound_count
    outbound = evidence.creator_outbound_count
    retention = evidence.creator_retention_ratio
    if (
        inbound is not None
        and outbound == 0
        and inbound > 0
        and retention is not None
        and retention >= FULL_CREATOR_RETENTION_RATIO
    ):
        reasons.append("creator_full_retention_blocked")
    if evidence.identity_count > 1:
        rank = evidence.identity_market_cap_rank
        if rank is None:
            reasons.append("identity_market_cap_incomplete")
        elif rank != 1:
            reasons.append("identity_lower_market_cap")
    return tuple(reasons)


def gmgn_safety_reasons(evidence: GmgnSafetyEvidence) -> Tuple[str, ...]:
    reasons = []
    if (
        evidence.market_cap_usd is not None
        and evidence.market_cap_usd <= MIN_MARKET_CAP_USD
    ):
        reasons.append("gmgn_market_cap_not_above_min")
    if evidence.is_honeypot is True:
        reasons.append("gmgn_honeypot")
    if evidence.is_open_source is False:
        reasons.append("gmgn_contract_unverified")
    if evidence.owner_renounced is False:
        reasons.append("gmgn_owner_not_renounced")
    if evidence.is_wash_trading is True:
        reasons.append("gmgn_wash_trading")
    for name, value, limit in (
        ("gmgn_rug_ratio_high", evidence.rug_ratio, MAX_GMGN_RISK_RATIO),
        ("gmgn_top_10_concentrated", evidence.top_10_holder_rate, MAX_GMGN_TOP_10_RATE),
        ("gmgn_bundler_high", evidence.bundler_rate, MAX_GMGN_RISK_RATIO),
        ("gmgn_insider_high", evidence.insider_rate, MAX_GMGN_RISK_RATIO),
        ("gmgn_entrapment_high", evidence.entrapment_ratio, MAX_GMGN_RISK_RATIO),
    ):
        if value is not None and value > limit:
            reasons.append(name)
    return tuple(reasons)


def gmgn_discovery_reasons(
    evidence: GmgnDiscoveryEvidence,
) -> Tuple[str, ...]:
    reasons = list(
        market_size_reasons(evidence.market_cap_usd, evidence.holder_count)
    )
    if evidence.liquidity_usd is None:
        reasons.append("gmgn_discovery_liquidity_unknown")
    elif evidence.liquidity_usd < MIN_GMGN_DISCOVERY_LIQUIDITY_USD:
        reasons.append("gmgn_discovery_liquidity_below_min")
    if evidence.volume_5m_usd is None:
        reasons.append("gmgn_discovery_volume_unknown")
    elif evidence.volume_5m_usd < MIN_GMGN_DISCOVERY_VOLUME_5M_USD:
        reasons.append("gmgn_discovery_volume_below_min")
    for name, value in (
        ("swaps", evidence.swaps_5m),
        ("buys", evidence.buys_5m),
        ("sells", evidence.sells_5m),
    ):
        if value is None:
            reasons.append("gmgn_discovery_%s_unknown" % name)
    if evidence.swaps_5m is not None and evidence.swaps_5m < 10:
        reasons.append("gmgn_discovery_swaps_below_min")
    if (
        evidence.buys_5m is not None
        and evidence.buys_5m < MIN_GMGN_DISCOVERY_BUYS_5M
    ):
        reasons.append("gmgn_discovery_buys_below_min")
    if evidence.sells_5m is not None and evidence.sells_5m < 1:
        reasons.append("gmgn_discovery_sells_below_min")
    if evidence.smart_degen_count is None:
        reasons.append("gmgn_discovery_smart_money_unknown")
    elif evidence.smart_degen_count < MIN_GMGN_DISCOVERY_SMART_MONEY:
        reasons.append("gmgn_discovery_smart_money_below_min")
    if evidence.creator_close is not True:
        reasons.append("gmgn_discovery_dev_not_confirmed_sold")
    if evidence.is_wash_trading is not False:
        reasons.append("gmgn_discovery_wash_status_not_safe")
    for name, value, limit in (
        ("rug", evidence.rug_ratio, MAX_GMGN_RISK_RATIO),
        ("top_10", evidence.top_10_holder_rate, MAX_GMGN_TOP_10_RATE),
        ("bundler", evidence.bundler_rate, MAX_GMGN_RISK_RATIO),
        ("insider", evidence.insider_rate, MAX_GMGN_RISK_RATIO),
        ("entrapment", evidence.entrapment_ratio, MAX_GMGN_RISK_RATIO),
        ("dev_team_hold", evidence.dev_team_hold_rate, MAX_GMGN_RISK_RATIO),
    ):
        if value is None:
            reasons.append("gmgn_discovery_%s_unknown" % name)
        elif value > limit:
            reasons.append("gmgn_discovery_%s_high" % name)
    if evidence.chain in {"bsc", "base"}:
        if evidence.is_honeypot is not False:
            reasons.append("gmgn_discovery_honeypot_status_not_safe")
        if evidence.is_open_source is not True:
            reasons.append("gmgn_discovery_contract_not_verified")
        if evidence.owner_renounced is not True:
            reasons.append("gmgn_discovery_owner_not_renounced")
    elif evidence.chain == "solana":
        if evidence.renounced_mint is not True:
            reasons.append("gmgn_discovery_mint_not_renounced")
        if evidence.renounced_freeze is not True:
            reasons.append("gmgn_discovery_freeze_not_renounced")
    else:
        reasons.append("gmgn_discovery_chain_unsupported")
    return tuple(reasons)
