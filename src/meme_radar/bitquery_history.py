from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Tuple

from .adapters import parse_fourmeme, parse_pons_launched
from .http_json import HttpJsonResult, JsonTransport
from .launchpads import FOURMEME_BSC_PROXY, PONS_V2_FACTORY
from .models import ParseBatch


QUERY = """
query($since: DateTime!, $limit: Int!) {
  EVM(network: %s) {
    Events(
      where: {
        Block: {Time: {after: $since}}
        LogHeader: {Address: {is: "%s"}}
        Log: {Signature: {Name: {is: "%s"}}}
      }
      orderBy: {ascending: Block_Time}
      limit: {count: $limit}
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
"""

SOURCES = {
    "fourmeme": (
        QUERY % ("bsc", FOURMEME_BSC_PROXY, "TokenCreate"),
        parse_fourmeme,
    ),
    "pons_events": (
        QUERY % ("robinhood", PONS_V2_FACTORY, "TokenLaunched"),
        parse_pons_launched,
    ),
}


def _iso(milliseconds: int) -> str:
    return datetime.fromtimestamp(
        milliseconds / 1000,
        tz=timezone.utc,
    ).isoformat()


class BitqueryHistorySource:
    URL = "https://streaming.bitquery.io/graphql"

    def __init__(self, transport: JsonTransport, token: str) -> None:
        if not token or "\r" in token or "\n" in token:
            raise ValueError("invalid Bitquery token")
        self._transport = transport
        self._authorization = "Bearer " + token

    def fetch(
        self,
        source_name: str,
        *,
        since_ms: int,
        limit: int = 500,
    ) -> Tuple[HttpJsonResult, ParseBatch]:
        if source_name not in SOURCES:
            raise ValueError("unknown Bitquery history source")
        if since_ms <= 0 or not 1 <= limit <= 1000:
            raise ValueError("invalid Bitquery history boundary")
        query, parser = SOURCES[source_name]
        result = self._transport.post_json(
            self.URL,
            {
                "query": query,
                "variables": {"since": _iso(since_ms), "limit": limit},
            },
            {"Authorization": self._authorization},
        )
        envelope = result.data
        if not isinstance(envelope, dict):
            raise RuntimeError("Bitquery history response invalid")
        if envelope.get("errors"):
            raise RuntimeError("Bitquery history GraphQL error")
        data = envelope.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Bitquery history data missing")
        batch = parser(
            data,
            observed_at_ms=result.received_at_ms,
            received_at_ms=result.received_at_ms,
            is_backfill=True,
        )
        return result, batch
