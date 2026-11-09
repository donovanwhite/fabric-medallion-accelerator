"""Compatibility discovery bridge for the explicit Treasury proof-case tests."""

from __future__ import annotations

import unittest

from use_cases.treasury.fixtures import test_synthetic_data as treasury_tests


def load_tests(
    loader: unittest.TestLoader, tests: unittest.TestSuite, pattern: str | None
) -> unittest.TestSuite:
    """Keep the original discovery command without duplicating test implementations."""
    return loader.loadTestsFromModule(treasury_tests)


def __getattr__(name: str) -> object:
    """Keep historical fully qualified Treasury test-class selection working."""
    if name not in {"ContractTests", "GenerationTests"}:
        raise AttributeError(name)
    return getattr(treasury_tests, name)


if __name__ == "__main__":
    unittest.main()
