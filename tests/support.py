import json
from pathlib import Path


FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as handle:
        return json.load(handle)
