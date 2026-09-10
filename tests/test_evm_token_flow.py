import unittest

from meme_radar.evm_token_flow import (
    TRANSFER_TOPIC,
    analyze_creator_transfers,
    collect_creator_flow,
)


TOKEN = "0x1111111111111111111111111111111111111111"
CREATOR = "0x2222222222222222222222222222222222222222"
OTHER = "0x3333333333333333333333333333333333333333"
ZERO = "0x0000000000000000000000000000000000000000"


def topic(address):
    return "0x" + "0" * 24 + address[2:]


def transfer(sender, recipient, amount, *, removed=False):
    return {
        "address": TOKEN,
        "topics": [TRANSFER_TOPIC, topic(sender), topic(recipient)],
        "data": "0x%064x" % amount,
        "removed": removed,
    }


class FakeClient:
    def __init__(self, logs):
        self.logs = logs
        self.log_calls = 0

    def call(self, method, params):
        if method == "eth_getLogs":
            self.log_calls += 1
            return self.logs if self.log_calls == 1 else []
        data = params[0]["data"]
        if data.startswith("0x70a08231"):
            return hex(60)
        if data == "0x18160ddd":
            return hex(1000)
        if data == "0x313ce567":
            return hex(18)
        raise AssertionError("unexpected call")


class CreatorFlowTests(unittest.TestCase):
    def test_creator_inbound_outbound_and_removed_logs(self):
        logs = [
            transfer(ZERO, CREATOR, 100),
            transfer(CREATOR, OTHER, 40),
            transfer(OTHER, CREATOR, 10, removed=True),
            transfer(OTHER, OTHER, 20),
        ]
        self.assertEqual(
            (1, 1, 100, 40),
            analyze_creator_transfers(
                logs,
                token_address=TOKEN,
                creator_address=CREATOR,
            ),
        )

    def test_collect_is_chunked_and_computes_retention(self):
        client = FakeClient(
            [transfer(ZERO, CREATOR, 100), transfer(CREATOR, OTHER, 40)]
        )
        evidence = collect_creator_flow(
            client,
            token_address=TOKEN,
            creator_address=CREATOR,
            from_block=10,
            to_block=29,
            chunk_blocks=10,
            max_span_blocks=100,
        )
        self.assertEqual(2, client.log_calls)
        self.assertEqual(1, evidence.inbound_count)
        self.assertEqual(1, evidence.outbound_count)
        self.assertEqual(0.6, evidence.retention_ratio)

    def test_range_is_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "boundary"):
            collect_creator_flow(
                FakeClient([]),
                token_address=TOKEN,
                creator_address=CREATOR,
                from_block=1,
                to_block=101,
                chunk_blocks=10,
                max_span_blocks=100,
            )


if __name__ == "__main__":
    unittest.main()
