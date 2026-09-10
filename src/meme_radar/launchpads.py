"""Versioned launchpad identities from documented public sources.

These constants identify candidate sources. Production admission still needs
an on-chain bytecode/event-topic verification probe.
"""

FOURMEME_BSC_PROXY = "0x5c952063c7fc8610ffdb798152d69f0b9550762b"
FOURMEME_TOKEN_CREATE_EVENT = "TokenCreate"

PONS_V2_FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
PONS_V2_ROUTER = "0xe33e9e479df8802cb0866d5d05258bec4cf62948"
PONS_V2_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
PONS_V2_LOCKER = "0x267444d099b10fb5ed7c3cc7b7c767adca574952"
PONS_V2_LAUNCH_SELECTORS = frozenset(
    {"0xf35abbcf", "0xa72101af", "0xf85f8e41"}
)

CLANKER_V4_BASE_FACTORY = "0xe85a59c628f7d27878aceb4bf3b35733630083a9"
CLANKER_V4_TOKEN_CREATED_TOPIC = (
    "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"
)
CLANKER_BASE_FACTORIES = frozenset({CLANKER_V4_BASE_FACTORY})

SOURCE_URLS = {
    "fourmeme": "https://docs.bitquery.io/docs/blockchain/BSC/binance-memerush-api/",
    "pons": "https://docs.bitquery.io/docs/blockchain/robinhood/pons-api/",
    "clanker": "https://www.clanker.world/api/metadata/factories",
}
