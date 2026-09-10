import json
import unittest

from meme_radar.evm_rpc import (
    FOURMEME_TOKEN_CREATE_TOPIC,
    NATIVE_SPECS,
    PONS_TOKEN_LAUNCHED_TOPIC,
    HttpRpcClient,
    RpcError,
    decode_abi_text,
    get_logs_with_413_split,
    parse_log_identity,
    parse_native_launch,
    validate_wss_endpoint,
)


NOW = 1_788_600_000_000


class RpcResponse:
    def __init__(self, payload):
        self.body = json.dumps(payload).encode("utf-8")
        self.headers = {
            "content-type": "application/json",
            "content-length": str(len(self.body)),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def getcode(self):
        return 200

    def read(self, _):
        body, self.body = self.body, b""
        return body


class BatchOpener:
    def open(self, request, timeout):
        del timeout
        payload = json.loads(request.data)
        results = []
        for item in payload:
            block_number = int(item["params"][0], 16)
            results.append(
                {
                    "jsonrpc": "2.0",
                    "id": item["id"],
                    "result": {
                        "number": hex(block_number),
                        "timestamp": hex(1000 + block_number),
                    },
                }
            )
        return RpcResponse(list(reversed(results)))


def topic_address(address):
    return "0x" + "0" * 24 + address[2:]


def log_for(chain):
    token = "0x1111111111111111111111111111111111111111"
    creator = "0x2222222222222222222222222222222222222222"
    spec = NATIVE_SPECS[chain]
    topics = [spec.topic0]
    data = "0x"
    if chain == "bsc":
        data += topic_address(creator)[2:] + topic_address(token)[2:]
    elif chain == "base":
        topics += [topic_address(token), topic_address(creator)]
    else:
        topics += [
            topic_address(token),
            topic_address("0x3333333333333333333333333333333333333333"),
            topic_address(creator),
        ]
    return {
        "address": spec.factory,
        "topics": topics,
        "data": data,
        "blockNumber": "0x10",
        "blockHash": "0x" + "a" * 64,
        "transactionHash": "0x" + "b" * 64,
        "logIndex": "0x2",
        "removed": False,
    }


class EvmRpcTests(unittest.TestCase):
    def test_bsc_log_chunks_fit_verified_provider_limit(self):
        self.assertEqual(5, NATIVE_SPECS["bsc"].max_log_blocks)
        self.assertEqual(100, NATIVE_SPECS["bsc"].history_limit_blocks)

    def test_log_payload_413_is_split_to_single_blocks(self):
        class DenseBlockClient:
            def __init__(self):
                self.calls = []

            def get_logs(self, _spec, lower, upper):
                self.calls.append((lower, upper))
                if lower != upper:
                    raise RpcError("HTTP_STATUS", 413)
                return [{"blockNumber": hex(lower)}]

        client = DenseBlockClient()
        logs, splits = get_logs_with_413_split(
            client,
            NATIVE_SPECS["bsc"],
            10,
            14,
        )
        self.assertEqual(
            [hex(value) for value in range(10, 15)],
            [item["blockNumber"] for item in logs],
        )
        self.assertEqual(4, splits)
        self.assertEqual(9, len(client.calls))

    def test_log_split_does_not_hide_non_payload_errors(self):
        class RateLimitedClient:
            def get_logs(self, _spec, _lower, _upper):
                raise RpcError("HTTP_STATUS", 429)

        with self.assertRaisesRegex(RpcError, "HTTP_STATUS"):
            get_logs_with_413_split(
                RateLimitedClient(),
                NATIVE_SPECS["bsc"],
                10,
                14,
            )

    def test_pinned_topics_match_verified_samples(self):
        self.assertEqual(
            "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20",
            FOURMEME_TOKEN_CREATE_TOPIC,
        )
        self.assertEqual(
            "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607",
            PONS_TOKEN_LAUNCHED_TOPIC,
        )

    def test_decodes_native_identity_for_all_three_chains(self):
        for chain in ("bsc", "base", "robinhood"):
            with self.subTest(chain=chain):
                identity = parse_log_identity(
                    log_for(chain),
                    NATIVE_SPECS[chain],
                )
                self.assertEqual(
                    "0x1111111111111111111111111111111111111111",
                    identity.token_address,
                )
                self.assertEqual(
                    "0x2222222222222222222222222222222222222222",
                    identity.creator,
                )
                self.assertEqual(16, identity.block_number)
                self.assertEqual(2, identity.log_index)

    def test_native_batch_hash_and_fail_closed_topic(self):
        log = log_for("base")
        batch = parse_native_launch(
            log,
            NATIVE_SPECS["base"],
            published_at_ms=NOW,
            observed_at_ms=NOW + 100,
            received_at_ms=NOW + 100,
            is_backfill=True,
        )
        self.assertEqual(1, len(batch.events))
        self.assertTrue(batch.events[0].is_backfill)
        self.assertEqual(batch.raw_sha256, batch.events[0].raw_sha256)
        bad = dict(log)
        bad["topics"] = ["0x" + "0" * 64] + log["topics"][1:]
        rejected = parse_native_launch(
            bad,
            NATIVE_SPECS["base"],
            published_at_ms=NOW,
            observed_at_ms=NOW + 100,
            received_at_ms=NOW + 100,
        )
        self.assertEqual(0, len(rejected.events))
        self.assertEqual("INVALID_NATIVE_RPC_LOG", rejected.issues[0].code)

    def test_abi_text_supports_dynamic_and_bytes32(self):
        text = b"Meme"
        dynamic = (
            (32).to_bytes(32, "big")
            + len(text).to_bytes(32, "big")
            + text.ljust(32, b"\x00")
        )
        self.assertEqual("Meme", decode_abi_text("0x" + dynamic.hex()))
        self.assertEqual(
            "MEME",
            decode_abi_text("0x" + b"MEME".ljust(32, b"\x00").hex()),
        )

    def test_endpoint_and_method_boundaries(self):
        HttpRpcClient("https://rpc.example.test/path")
        validate_wss_endpoint("wss://rpc.example.test/path")
        with self.assertRaisesRegex(ValueError, "endpoint"):
            HttpRpcClient("http://rpc.example.test")
        with self.assertRaisesRegex(ValueError, "endpoint"):
            validate_wss_endpoint("wss://rpc.example.test/path?x=bad")
        client = HttpRpcClient("https://rpc.example.test/path")
        with self.assertRaisesRegex(ValueError, "method"):
            client.call("eth_sendRawTransaction", [])

    def test_batch_block_timestamps_are_bounded_and_order_independent(self):
        client = HttpRpcClient("https://rpc.example.test/path")
        client._opener = BatchOpener()
        self.assertEqual(
            {16: 1_016_000, 17: 1_017_000},
            client.block_timestamps_ms([16, 17, 16]),
        )
        with self.assertRaisesRegex(ValueError, "batch"):
            client.call_batch([])
        with self.assertRaisesRegex(ValueError, "method"):
            client.call_batch([("eth_sendRawTransaction", [])])


if __name__ == "__main__":
    unittest.main()
