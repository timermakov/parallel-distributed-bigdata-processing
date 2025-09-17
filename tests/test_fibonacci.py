import sys
import os
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.main import fib_fast_doubling


@pytest.mark.parametrize("n, expected", [
    (0, 0),
    (1, 1),
    (2, 1),
    (5, 5),
    (10, 55),
    (20, 6765),
])
def test_fib_fast_doubling(n: int, expected: int) -> None:
    assert fib_fast_doubling(n) == expected


def test_negative_raises() -> None:
    with pytest.raises(ValueError):
        fib_fast_doubling(-1)
