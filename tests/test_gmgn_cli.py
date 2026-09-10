import json
import subprocess
import unittest

from meme_radar.sources.gmgn_cli import (
    GmgnCliError,
    GmgnTokenInfoSource,
    GmgnSolanaSecuritySource,
    GmgnTrendingSource,
    GmgnTrenchesSource,
)


TOKEN = "0x1111111111111111111111111111111111111111"
SOL_TOKEN = "So11111111111111111111111111111111111111112"


class Clock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value


class GmgnCliTests(unittest.TestCase):
    @staticmethod
    def trending_row(chain="bsc", address=TOKEN):
        return {
            "chain": chain,
            "address": address,
            "name": "Growth Meme",
            "symbol": "GROW",
            "creator": "0x2222222222222222222222222222222222222222",
            "launchpad_platform": "fourmeme",
            "creation_timestamp": 1_788_999_400,
            "holder_count": 250,
            "market_cap": 250_000,
            "liquidity": 60_000,
            "volume": 25_000,
            "swaps": 120,
            "buys": 80,
            "sells": 40,
            "smart_degen_count": 5,
            "renowned_count": 2,
            "creator_close": True,
            "is_honeypot": 0,
            "is_open_source": 1,
            "is_renounced": 1,
            "is_wash_trading": False,
            "rug_ratio": 0.1,
            "top_10_holder_rate": 0.2,
            "bundler_rate": 0.1,
            "rat_trader_amount_rate": 0.1,
            "entrapment_ratio": 0.1,
            "dev_team_hold_rate": 0.1,
        }

    def test_trending_discovery_parses_strict_growth_candidate(self):
        calls = []

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            payload = {"code": 0, "data": {"rank": [self.trending_row()]}}
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        result = GmgnTrendingSource(
            runner=runner,
            wall_time=lambda: 1_789_000_000,
        ).fetch("bsc")
        self.assertEqual(1, len(result.batch.events))
        self.assertEqual(1, len(result.snapshots))
        self.assertEqual("gmgn_trending_v2", result.batch.events[0].source)
        snapshot = result.snapshots[0]
        self.assertEqual(250, snapshot.holder_count)
        self.assertEqual(250_000, snapshot.market_cap_usd)
        self.assertTrue(snapshot.creator_close)
        command, kwargs = next(
            item for item in calls if item[0][1] == "market"
        )
        self.assertIn("--min-marketcap", command)
        self.assertIn("--min-holder-count", command)
        self.assertIn("not_honeypot", command)
        self.assertNotIn("shell", kwargs)
        self.assertNotIn("GMGN_PRIVATE_KEY", kwargs["env"])

    def test_trending_discovery_rejects_server_filter_mismatch(self):
        row = self.trending_row()
        row["holder_count"] = 100

        def runner(command, **_kwargs):
            payload = {"code": 0, "data": {"rank": [row]}}
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        result = GmgnTrendingSource(
            runner=runner,
            wall_time=lambda: 1_789_000_000,
        ).fetch("bsc")
        self.assertEqual((), result.batch.events)
        self.assertEqual("INVALID_ROW", result.batch.issues[0].code)

    def test_solana_trending_uses_chain_specific_filters(self):
        sol = "So11111111111111111111111111111111111111112"
        row = self.trending_row("sol", sol)
        row["creator"] = "11111111111111111111111111111111"
        row["renounced_mint"] = 1
        row["renounced_freeze_account"] = 1

        def runner(command, **_kwargs):
            if command[1] == "config":
                return subprocess.CompletedProcess(
                    command, 0, stdout=b"", stderr=b""
                )
            self.assertIn("sol", command)
            self.assertIn("frozen", command)
            self.assertIn("not_wash_trading", command)
            payload = {"code": 0, "data": {"rank": [row]}}
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        result = GmgnTrendingSource(
            runner=runner,
            wall_time=lambda: 1_789_000_000,
        ).fetch("solana")
        self.assertEqual("solana", result.snapshots[0].chain)
        self.assertTrue(result.snapshots[0].renounced_mint)

    def test_trending_rate_limit_is_explicit_after_config_check(self):
        def runner(command, **_kwargs):
            if command[1] == "config":
                return subprocess.CompletedProcess(
                    command, 0, stdout=b"", stderr=b""
                )
            return subprocess.CompletedProcess(
                command,
                1,
                stdout=b"",
                stderr=b"GET failed: HTTP 429",
            )

        with self.assertRaises(GmgnCliError) as raised:
            GmgnTrendingSource(runner=runner).fetch("base")
        self.assertEqual("RATE_LIMIT", raised.exception.code)
        self.assertEqual(429, raised.exception.status)

    def test_token_info_computes_market_cap_and_holders(self):
        payload = {
            "address": TOKEN,
            "symbol": "TEST",
            "holder_count": 101,
            "circulating_supply": "1000000",
            "liquidity": "50000",
            "price": {"price": "0.2"},
        }
        calls = []

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        source = GmgnTokenInfoSource(runner=runner)
        snapshot = source.token_info("base", TOKEN)
        self.assertEqual(101, snapshot.holder_count)
        self.assertEqual(200_000, snapshot.market_cap_usd)
        self.assertEqual(0.2, snapshot.price_usd)
        self.assertEqual("token", calls[0][0][1])
        self.assertNotIn("GMGN_PRIVATE_KEY", calls[0][1]["env"])

    def test_token_info_empty_symbol_is_not_covered(self):
        def runner(command, **_kwargs):
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=json.dumps({"symbol": "", "holder_count": 0}).encode(),
                stderr=b"",
            )

        self.assertIsNone(GmgnTokenInfoSource(runner=runner).token_info("bsc", TOKEN))

    def test_solana_token_info_uses_cli_chain_and_total_fee(self):
        calls = []
        payload = {
            "address": SOL_TOKEN,
            "symbol": "SOLTEST",
            "holder_count": 80,
            "market_cap": "45000",
            "liquidity": "18000",
            "total_fee": "2.5",
            "price": {"price": "0.0045"},
        }

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        snapshot = GmgnTokenInfoSource(runner=runner).token_info("solana", SOL_TOKEN)
        self.assertEqual("solana", snapshot.chain)
        self.assertEqual(45_000, snapshot.market_cap_usd)
        self.assertEqual(2.5, snapshot.total_fee)
        self.assertEqual("sol", calls[0][0][4])
        self.assertNotIn("GMGN_PRIVATE_KEY", calls[0][1]["env"])

    def test_solana_security_requires_explicit_fields(self):
        payload = {
            "data": {
                "token": {
                    "is_honeypot": None,
                    "honeypot": 0,
                    "is_blacklist": None,
                    "blacklist": 0,
                    "top_10_holder_rate": "0.22",
                    "renounced_mint": True,
                    "renounced_freeze_account": True,
                }
            }
        }
        calls = []

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        result = GmgnSolanaSecuritySource(runner=runner).token_security(SOL_TOKEN)
        self.assertEqual("normal", result.risk_level)
        self.assertFalse(result.is_blacklisted)
        self.assertEqual(0.22, result.top_10_holder_rate)
        self.assertTrue(result.renounced_mint)
        self.assertTrue(result.renounced_freeze)
        self.assertEqual("security", calls[0][0][2])
        self.assertNotIn("GMGN_PRIVATE_KEY", calls[0][1]["env"])

    def test_solana_security_explicit_high_risk_overrides_false_flags(self):
        payload = {
            "address": SOL_TOKEN,
            "risk_level": "high",
            "is_honeypot": False,
            "is_blacklisted": False,
        }

        def runner(command, **_kwargs):
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        result = GmgnSolanaSecuritySource(runner=runner).token_security(SOL_TOKEN)
        self.assertEqual("high", result.risk_level)

    def test_solana_security_unknown_explicit_risk_never_falls_back_normal(self):
        payload = {
            "address": SOL_TOKEN,
            "risk_level": "warning",
            "is_honeypot": False,
            "is_blacklisted": False,
        }

        def runner(command, **_kwargs):
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        result = GmgnSolanaSecuritySource(runner=runner).token_security(SOL_TOKEN)
        self.assertEqual("unrecognized", result.risk_level)

    def test_solana_token_info_rejects_response_identity_mismatch(self):
        payload = {
            "address": "11111111111111111111111111111111",
            "symbol": "WRONG",
        }

        def runner(command, **_kwargs):
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        with self.assertRaises(GmgnCliError) as raised:
            GmgnTokenInfoSource(runner=runner).token_info("solana", SOL_TOKEN)
        self.assertEqual("IDENTITY_MISMATCH", raised.exception.code)

    def test_bsc_snapshot_is_parsed_and_cached_without_shell(self):
        calls = []
        payload = {
            "completed": [
                {
                    "address": TOKEN,
                    "market_cap": "25000",
                    "total_fee": "0.04",
                    "creator_token_status": "creator_hold",
                    "creator_balance_rate": "0.01",
                    "is_honeypot": "no",
                    "open_source": "yes",
                    "owner_renounced": "yes",
                    "rug_ratio": "0.1",
                    "top_10_holder_rate": "0.2",
                    "bundler_trader_amount_rate": "0.05",
                    "rat_trader_amount_rate": "0.03",
                }
            ]
        }

        def runner(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=json.dumps(payload).encode(),
                stderr=b"",
            )

        source = GmgnTrenchesSource(runner=runner, monotonic=Clock())
        first = source.token_safety("bsc", TOKEN)
        second = source.token_safety("bsc", TOKEN)
        self.assertEqual(25_000, first.market_cap_usd)
        self.assertFalse(first.is_honeypot)
        self.assertEqual(first, second)
        self.assertEqual(1, len(calls))
        command, kwargs = calls[0]
        self.assertEqual("/usr/local/bin/gmgn-cli", command[0])
        self.assertNotIn("shell", kwargs)
        self.assertNotIn("GMGN_PRIVATE_KEY", kwargs["env"])
        self.assertEqual("/", kwargs["cwd"])

    def test_base_reads_both_lifecycle_categories(self):
        payload = {
            "new_creation": [{"address": TOKEN, "market_cap": "21000"}],
            "completed": [{"address": TOKEN, "market_cap": "31000"}],
        }

        def runner(command, **_kwargs):
            self.assertIn("new_creation", command)
            self.assertIn("completed", command)
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        snapshot = GmgnTrenchesSource(runner=runner).token_safety("base", TOKEN)
        self.assertEqual(31_000, snapshot.market_cap_usd)

    def test_solana_trenches_reads_complete_quality_fields(self):
        row = {
            "chain": "sol",
            "address": SOL_TOKEN,
            "market_cap": "45000",
            "liquidity": "18000",
            "total_fee": "2.5",
            "creator_token_status": "creator_hold",
            "rug_ratio": "0.1",
            "top_10_holder_rate": "0.2",
            "bundler_trader_amount_rate": "0.05",
            "rat_trader_amount_rate": "0.03",
            "entrapment_ratio": "0.04",
            "dev_team_hold_rate": "0.02",
            "renounced_mint": True,
            "renounced_freeze_account": True,
        }

        def runner(command, **_kwargs):
            self.assertEqual("sol", command[command.index("--chain") + 1])
            for category in ("new_creation", "near_completion", "completed"):
                self.assertIn(category, command)
            self.assertNotIn("--launchpad-platform", command)
            for value in (
                "--max-created",
                "--min-marketcap",
                "--max-marketcap",
                "--min-liquidity",
                "--min-total-fee",
                "--max-total-fee",
            ):
                self.assertIn(value, command)
            payload = {"near_completion": [row]}
            return subprocess.CompletedProcess(
                command, 0, stdout=json.dumps(payload).encode(), stderr=b""
            )

        snapshot = GmgnTrenchesSource(runner=runner).token_safety(
            "solana", SOL_TOKEN
        )
        self.assertEqual(45_000, snapshot.market_cap_usd)
        self.assertEqual(18_000, snapshot.liquidity_usd)
        self.assertEqual(2.5, snapshot.total_fee)
        self.assertEqual(0.02, snapshot.dev_team_hold_rate)
        self.assertTrue(snapshot.renounced_mint)
        self.assertTrue(snapshot.renounced_freeze)

    def test_auth_and_rate_limit_are_explicit(self):
        for status, code in (
            (401, "AUTH_ERROR"),
            (403, "AUTH_ERROR"),
            (429, "RATE_LIMIT"),
        ):
            with self.subTest(status=status):
                def runner(command, **_kwargs):
                    return subprocess.CompletedProcess(
                        command,
                        1,
                        stdout=b"",
                        stderr=("GET failed: HTTP %d" % status).encode(),
                    )

                with self.assertRaises(GmgnCliError) as raised:
                    GmgnTrenchesSource(runner=runner).token_safety("bsc", TOKEN)
                self.assertEqual(code, raised.exception.code)
                self.assertEqual(status, raised.exception.status)


if __name__ == "__main__":
    unittest.main()
