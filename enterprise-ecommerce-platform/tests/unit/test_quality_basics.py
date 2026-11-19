
import pytest


def is_valid_payment(amount):
    """Return True when payment is not negative."""
    return amount is not None and amount >= 0


def test_valid_payment():
    assert is_valid_payment(150.0) is True


def test_negative_payment():
    assert is_valid_payment(-50.0) is False


def test_null_payment():
    assert is_valid_payment(None) is False


def test_zero_payment():
    assert is_valid_payment(0) is True
