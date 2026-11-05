"""Tests for the shared rate limiter and the Gemini retry helpers.

No database or network access is needed.

    python -m unittest backend.tests.test_rate_limiter
"""

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

from backend.data.RateLimiter import (
    RateLimiter,
    call_with_retries,
    get_rate_limiter,
    is_retryable,
    quota_hint,
    retry_after_seconds,
)


class FakeApiError(Exception):
    """Shape-compatible stand-in for google.genai.errors.ClientError."""

    def __init__(self, code=429, status="RESOURCE_EXHAUSTED", details=None):
        super().__init__(f"{code} {status}")
        self.code = code
        self.status = status
        self.details = details or {}


def rate_limit_details(rpm=15, retry_delay="0s", quota_id="GenerateRequestsPerMinutePerProjectPerModel-FreeTier"):
    return {
        "error": {
            "details": [
                {
                    "@type": "type.googleapis.com/google.rpc.QuotaFailure",
                    "violations": [
                        {
                            "quotaId": quota_id,
                            "quotaMetric": "generativelanguage.googleapis.com/generate_content_free_tier_requests",
                            "quotaValue": str(rpm),
                        }
                    ],
                },
                {
                    "@type": "type.googleapis.com/google.rpc.RetryInfo",
                    "retryDelay": retry_delay,
                },
            ]
        }
    }


class PersistenceTests(unittest.TestCase):
    def test_window_survives_a_new_limiter(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "shared.json"
            first = RateLimiter(100, 1_000_000, scope="test-shared", state_file=state)
            asyncio.run(first.acquire(10))
            asyncio.run(first.acquire(10))
            first.flush()

            second = RateLimiter(100, 1_000_000, scope="test-shared", state_file=state)
            self.assertEqual(second._get_current_usage(), (2, 20))

    def test_stale_window_is_discarded(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "stale.json"
            old = time.time() - 120
            state.write_text(
                json.dumps({"requests": [old, old], "tokens": [[old, 10], [old, 10]]}),
                encoding="utf-8",
            )
            limiter = RateLimiter(100, 1_000_000, scope="test-stale", state_file=state)
            self.assertEqual(limiter._get_current_usage(), (0, 0))

    def test_next_stage_is_throttled_by_the_previous_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "race.json"
            first = RateLimiter(2, 1_000_000, scope="test-race", state_file=state)
            asyncio.run(first.acquire(1))
            asyncio.run(first.acquire(1))
            first.flush()

            second = RateLimiter(2, 1_000_000, scope="test-race", state_file=state)

            async def must_wait():
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(second.acquire(1), timeout=0.3)

            asyncio.run(must_wait())


class SingletonTests(unittest.TestCase):
    def test_same_scope_returns_the_same_instance(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "singleton.json"
            scope = f"test-singleton-{time.time()}"
            one = get_rate_limiter(10, 1000, scope=scope, state_file=state)
            two = get_rate_limiter(10, 1000, scope=scope, state_file=state)
            other = get_rate_limiter(10, 2000, scope=scope, state_file=state)
            self.assertIs(one, two)
            self.assertIsNot(one, other)


class RetryHelperTests(unittest.TestCase):
    def test_is_retryable(self):
        self.assertTrue(is_retryable(FakeApiError(code=429)))
        self.assertTrue(is_retryable(FakeApiError(code=503, status="UNAVAILABLE")))
        self.assertFalse(is_retryable(ValueError("schema mismatch")))

    def test_retry_after_seconds_reads_retry_info(self):
        exc = FakeApiError(details=rate_limit_details(retry_delay="52.775139194s"))
        self.assertAlmostEqual(retry_after_seconds(exc), 52.775139194)

    def test_quota_hint_prefers_the_per_minute_request_quota(self):
        exc = FakeApiError(details=rate_limit_details(rpm=15))
        self.assertEqual(quota_hint(exc), ("requests", 15))

    def test_daily_quota_is_not_used_as_rpm(self):
        exc = FakeApiError(
            details=rate_limit_details(
                rpm=1000, quota_id="GenerateRequestsPerDayPerProjectPerModel-FreeTier"
            )
        )
        self.assertIsNone(quota_hint(exc))

    def test_retries_then_succeeds_and_tightens_the_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            limiter = RateLimiter(3850, 3_850_000, scope="test-retry", state_file=Path(tmp) / "r.json")
            calls = {"n": 0}

            async def request():
                calls["n"] += 1
                if calls["n"] < 3:
                    raise FakeApiError(details=rate_limit_details(rpm=15))
                return "ok"

            result = asyncio.run(
                call_with_retries(
                    request, rate_limiter=limiter, estimated_tokens=5, attempts=4
                )
            )
            self.assertEqual(result, "ok")
            self.assertEqual(calls["n"], 3)
            self.assertEqual(limiter.rpm_limit, 15)
            self.assertEqual(limiter._get_current_usage(), (3, 15))

    def test_gives_up_after_the_last_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            limiter = RateLimiter(3850, 3_850_000, scope="test-giveup", state_file=Path(tmp) / "r.json")
            calls = {"n": 0}

            async def request():
                calls["n"] += 1
                raise FakeApiError(details=rate_limit_details(rpm=15))

            with self.assertRaises(FakeApiError):
                asyncio.run(
                    call_with_retries(
                        request, rate_limiter=limiter, estimated_tokens=5, attempts=3
                    )
                )
            self.assertEqual(calls["n"], 3)


if __name__ == "__main__":
    unittest.main()
