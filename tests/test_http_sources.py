import io
import json
import unittest
from email.message import Message

from meme_radar.http_json import (
    BoundedJsonClient,
    HttpBoundaryError,
    HttpJsonResult,
)
from meme_radar.launchpads import (
    CLANKER_V4_BASE_FACTORY,
    CLANKER_V4_TOKEN_CREATED_TOPIC,
)
from meme_radar.sources import ClankerPublicSource, FomoPublicSource
from meme_radar.sources import (
    DexScreenerSource,
    GoPlusSource,
    ProviderApiError,
    ProviderUnavailable,
)

from support import load_fixture


NOW = 1_788_516_100_000


class FakeResponse:
    def __init__(
        self,
        body,
        content_type="application/json",
        status=200,
        content_length=None,
    ):
        self._stream = io.BytesIO(body)
        self._status = status
        self.headers = Message()
        self.headers["content-type"] = content_type
        if content_length is not None:
            self.headers["content-length"] = str(content_length)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def getcode(self):
        return self._status

    def read(self, size):
        return self._stream.read(size)


class FakeOpener:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return self.response


class FakeTransport:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def get_json(self, url, headers=None):
        self.calls.append((url, dict(headers or {})))
        if url not in self.mapping:
            raise AssertionError("unexpected URL " + url)
        return HttpJsonResult(
            data=self.mapping[url],
            status=200,
            bytes_read=10,
            elapsed_ms=1,
            started_at_ms=NOW - 10,
            received_at_ms=NOW,
            rate_limit={},
        )

    def post_json(self, url, payload, headers=None):
        self.calls.append((url, dict(headers or {}), payload))
        if url not in self.mapping:
            raise AssertionError("unexpected URL " + url)
        return HttpJsonResult(
            data=self.mapping[url],
            status=200,
            bytes_read=10,
            elapsed_ms=1,
            started_at_ms=NOW - 10,
            received_at_ms=NOW,
            rate_limit={},
        )


class HttpBoundaryTests(unittest.TestCase):
    def test_reads_valid_json_with_byte_count(self):
        body = b'{"ok":true}'
        opener = FakeOpener(FakeResponse(body))
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            max_bytes=len(body),
            opener=opener,
        )
        result = client.get_json("https://example.test/data")
        self.assertEqual({"ok": True}, result.data)
        self.assertEqual(len(body), result.bytes_read)

    def test_rejects_unlisted_host_and_non_json(self):
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            opener=FakeOpener(FakeResponse(b"ok", "text/plain")),
        )
        with self.assertRaisesRegex(HttpBoundaryError, "URL_NOT_ALLOWED"):
            client.get_json("https://other.test/data")
        with self.assertRaisesRegex(HttpBoundaryError, "CONTENT_TYPE"):
            client.get_json("https://example.test/data")

    def test_rejects_nonstandard_port_and_unapproved_header(self):
        opener = FakeOpener(FakeResponse(b'{"ok":true}'))
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            opener=opener,
        )
        with self.assertRaisesRegex(HttpBoundaryError, "URL_NOT_ALLOWED"):
            client.get_json("https://example.test:444/data")
        with self.assertRaisesRegex(HttpBoundaryError, "HEADER_NOT_ALLOWED"):
            client.get_json(
                "https://example.test/data",
                {"Cookie": "session=secret"},
            )
        self.assertEqual([], opener.requests)

    def test_rejects_invalid_content_length(self):
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            opener=FakeOpener(
                FakeResponse(b'{"ok":true}', content_length=-1)
            ),
        )
        with self.assertRaisesRegex(HttpBoundaryError, "INVALID_CONTENT_LENGTH"):
            client.get_json("https://example.test/data")

    def test_rejects_chunked_body_over_limit(self):
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            max_bytes=4,
            opener=FakeOpener(FakeResponse(b'{"large":true}')),
        )
        with self.assertRaisesRegex(HttpBoundaryError, "BODY_TOO_LARGE"):
            client.get_json("https://example.test/data")

    def test_rejects_duplicate_json_keys(self):
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            opener=FakeOpener(FakeResponse(b'{"ok":1,"ok":2}')),
        )
        with self.assertRaisesRegex(HttpBoundaryError, "INVALID_JSON"):
            client.get_json("https://example.test/data")

    def test_post_json_is_bounded_and_sets_method(self):
        opener = FakeOpener(FakeResponse(b'{"ok":true}'))
        client = BoundedJsonClient(
            allowed_hosts={"example.test"},
            opener=opener,
        )
        result = client.post_json("https://example.test/data", {"b": 2, "a": 1})
        request = opener.requests[0][0]
        self.assertEqual({"ok": True}, result.data)
        self.assertEqual("POST", request.get_method())
        self.assertEqual(b'{"a":1,"b":2}', request.data)


class SourceClientTests(unittest.TestCase):
    def test_fomo_public_alerts_need_no_key(self):
        url = "https://api.fomoapi.io/v2/alerts?limit=20"
        source = FomoPublicSource(
            FakeTransport({url: load_fixture("fomo_alerts.json")})
        )
        batch = source.fetch_alerts()
        self.assertEqual(1, len(batch.events))
        self.assertEqual(1, len(batch.issues))

    def test_fomo_token_board_requires_key_before_io(self):
        transport = FakeTransport({})
        source = FomoPublicSource(transport)
        with self.assertRaisesRegex(RuntimeError, "key required"):
            source.fetch_token_board("trending")
        self.assertEqual([], transport.calls)

    def test_fomo_rejects_header_injection_before_io(self):
        transport = FakeTransport({})
        source = FomoPublicSource(transport, api_key="bad\nkey")
        with self.assertRaisesRegex(ValueError, "invalid FOMO API key"):
            source.fetch_alerts()
        self.assertEqual([], transport.calls)

    def test_clanker_factory_metadata_is_pinned(self):
        meta_url = "https://www.clanker.world/api/metadata/factories"
        token_url = (
            "https://www.clanker.world/api/tokens?"
            "chainId=8453&startDate=400&limit=20&sort=desc"
        )
        metadata = [
            {
                "name": "clanker_v4_base",
                "address": CLANKER_V4_BASE_FACTORY,
                "events": {
                    "tokenCreated": CLANKER_V4_TOKEN_CREATED_TOPIC,
                },
            }
        ]
        source = ClankerPublicSource(
            FakeTransport(
                {
                    meta_url: metadata,
                    token_url: load_fixture("clanker_tokens.json"),
                }
            ),
            now=lambda: 1000,
        )
        batch = source.fetch_latest_base()
        self.assertEqual(1, len(batch.events))
        self.assertEqual(1, len(batch.issues))

    def test_clanker_metadata_drift_fails_closed(self):
        meta_url = "https://www.clanker.world/api/metadata/factories"
        source = ClankerPublicSource(
            FakeTransport(
                {
                    meta_url: [
                        {
                            "name": "clanker_v4_base",
                            "address": CLANKER_V4_BASE_FACTORY,
                            "events": {"tokenCreated": "0x" + "0" * 64},
                        }
                    ]
                }
            )
        )
        with self.assertRaisesRegex(RuntimeError, "metadata changed"):
            source.fetch_latest_base()

    def test_goplus_token_is_cached_and_not_double_prefixed(self):
        token_url = "https://api.gopluslabs.io/api/v1/token"
        security_url = (
            "https://api.gopluslabs.io/api/v1/token_security/56?"
            "contract_addresses=0x1111111111111111111111111111111111111111"
        )
        transport = FakeTransport(
            {
                token_url: {
                    "code": 1,
                    "message": "ok",
                    "result": {
                        "access_token": "Bearer a.b.c",
                        "expires_in": 7200,
                    },
                },
                security_url: {
                    "code": 1,
                    "message": "OK",
                    "result": {
                        "0x1111111111111111111111111111111111111111": {
                            "is_honeypot": "0"
                        }
                    },
                },
            }
        )
        source = GoPlusSource(
            transport,
            app_key="app",
            app_secret="secret",
            now=lambda: 1000,
        )
        first = source.token_security(
            "bsc", "0x1111111111111111111111111111111111111111"
        )
        second = source.token_security(
            "bsc", "0x1111111111111111111111111111111111111111"
        )
        token_calls = [call for call in transport.calls if call[0] == token_url]
        security_calls = [call for call in transport.calls if call[0] == security_url]
        self.assertEqual(1, len(token_calls))
        self.assertEqual(2, len(security_calls))
        self.assertEqual(
            "Bearer a.b.c",
            security_calls[0][1]["Authorization"],
        )
        self.assertEqual("0", first.payload["is_honeypot"])
        self.assertEqual(first, second)

    def test_goplus_evm_client_rejects_solana_before_io(self):
        transport = FakeTransport({})
        source = GoPlusSource(
            transport,
            app_key="app",
            app_secret="secret",
            now=lambda: 1000,
        )
        with self.assertRaisesRegex(ValueError, "EVM"):
            source.token_security(
                "solana",
                "So11111111111111111111111111111111111111112",
            )
        self.assertEqual([], transport.calls)

    def test_goplus_missing_token_is_unavailable(self):
        token_url = "https://api.gopluslabs.io/api/v1/token"
        security_url = (
            "https://api.gopluslabs.io/api/v1/token_security/56?"
            "contract_addresses=0x1111111111111111111111111111111111111111"
        )
        source = GoPlusSource(
            FakeTransport(
                {
                    token_url: {
                        "code": 1,
                        "result": {
                            "access_token": "Bearer a.b.c",
                            "expires_in": 7200,
                        },
                    },
                    security_url: {"code": 1, "result": {}},
                }
            ),
            app_key="app",
            app_secret="secret",
            now=lambda: 1000,
        )
        with self.assertRaises(ProviderUnavailable):
            source.token_security(
                "bsc",
                "0x1111111111111111111111111111111111111111",
            )

    def test_goplus_business_error_keeps_safe_api_code(self):
        token_url = "https://api.gopluslabs.io/api/v1/token"
        security_url = (
            "https://api.gopluslabs.io/api/v1/token_security/56?"
            "contract_addresses=0x1111111111111111111111111111111111111111"
        )
        source = GoPlusSource(
            FakeTransport(
                {
                    token_url: {
                        "code": 1,
                        "result": {
                            "access_token": "Bearer a.b.c",
                            "expires_in": 7200,
                        },
                    },
                    security_url: {"code": 2021, "message": "pending"},
                }
            ),
            app_key="app",
            app_secret="secret",
            now=lambda: 1000,
        )
        with self.assertRaises(ProviderApiError) as caught:
            source.token_security(
                "bsc",
                "0x1111111111111111111111111111111111111111",
            )
        self.assertEqual(2021, caught.exception.api_code)

    def test_dexscreener_selects_highest_liquidity_pair(self):
        url = (
            "https://api.dexscreener.com/latest/dex/tokens/"
            "0x1111111111111111111111111111111111111111"
        )
        transport = FakeTransport(
            {
                url: {
                    "pairs": [
                        {
                            "chainId": "bsc",
                            "baseToken": {"address": "0x1111111111111111111111111111111111111111"},
                            "pairAddress": "low",
                            "priceUsd": "1",
                            "liquidity": {"usd": 10},
                            "volume": {"h24": 5},
                        },
                        {
                            "chainId": "bsc",
                            "baseToken": {"address": "0x1111111111111111111111111111111111111111"},
                            "pairAddress": "high",
                            "pairCreatedAt": 1234567890,
                            "priceUsd": "2",
                            "marketCap": "25000",
                            "fdv": "30000",
                            "liquidity": {"usd": 100},
                            "volume": {"m5": 7, "h1": 20, "h24": 50},
                            "txns": {
                                "m5": {"buys": 3, "sells": 1},
                                "h1": {"buys": 8, "sells": 3},
                                "h24": {"buys": 12, "sells": 5},
                            },
                        },
                        {
                            "chainId": "base",
                            "baseToken": {"address": "0x1111111111111111111111111111111111111111"},
                            "pairAddress": "wrong-chain",
                            "liquidity": {"usd": 1000},
                        },
                    ]
                }
            }
        )
        snapshot = DexScreenerSource(transport).token_market(
            "bsc", "0x1111111111111111111111111111111111111111"
        )
        self.assertEqual(2, snapshot.pair_count)
        self.assertEqual("high", snapshot.best_pair_address)
        self.assertEqual(1234567890, snapshot.pair_created_at_ms)
        self.assertEqual(2.0, snapshot.price_usd)
        self.assertEqual(100.0, snapshot.liquidity_usd)
        self.assertEqual(7.0, snapshot.volume_5m_usd)
        self.assertEqual(25_000.0, snapshot.market_cap_usd)
        self.assertEqual(30_000.0, snapshot.fdv_usd)
        self.assertEqual(3, snapshot.buy_transactions_5m)
        self.assertEqual(8, snapshot.buy_transactions_1h)
        self.assertEqual(12, snapshot.buy_transactions_24h)
        self.assertEqual(5, snapshot.sell_transactions_24h)

    def test_dexscreener_null_pairs_is_unavailable_not_error(self):
        url = (
            "https://api.dexscreener.com/latest/dex/tokens/"
            "0x1111111111111111111111111111111111111111"
        )
        snapshot = DexScreenerSource(
            FakeTransport({url: {"pairs": None}})
        ).token_market(
            "bsc",
            "0x1111111111111111111111111111111111111111",
        )
        self.assertEqual(0, snapshot.pair_count)
        self.assertIsNone(snapshot.price_usd)

    def test_dexscreener_rejects_pair_for_different_token(self):
        token = "0x1111111111111111111111111111111111111111"
        url = "https://api.dexscreener.com/latest/dex/tokens/" + token
        payload = {
            "pairs": [
                {
                    "chainId": "bsc",
                    "baseToken": {"address": "0x2222222222222222222222222222222222222222"},
                    "quoteToken": {"address": "0x3333333333333333333333333333333333333333"},
                    "pairAddress": "wrong-token",
                    "liquidity": {"usd": 999999},
                }
            ]
        }
        snapshot = DexScreenerSource(FakeTransport({url: payload})).token_market(
            "bsc", token
        )
        self.assertEqual(0, snapshot.pair_count)


if __name__ == "__main__":
    unittest.main()
