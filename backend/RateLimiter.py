import asyncio
import time
from collections import deque
from typing import Tuple


class RateLimiter:
    """
    Token-aware rate limiter for API calls.
    Tracks requests per minute (RPM) and tokens per minute (TPM).
    """

    def __init__(self, rpm_limit: int = 480, tpm_limit: int = 480000):
        """
        Initialize rate limiter with conservative limits.

        GPT-5.1 free tier:
        - 500 RPM
        - 500,000 TPM

        We use 480/180k to leave a safety buffer.
        """
        self.rpm_limit = rpm_limit
        self.tpm_limit = tpm_limit

        self.request_times = deque()  # Timestamps of requests
        self.token_times = deque()  # (timestamp, token_count) tuples

        self.lock = asyncio.Lock()
        self.total_requests = 0
        self.total_tokens = 0

    def _clean_old_entries(self):
        """Remove entries older than 60 seconds."""
        cutoff = time.time() - 60

        while self.request_times and self.request_times[0] < cutoff:
            self.request_times.popleft()

        while self.token_times and self.token_times[0][0] < cutoff:
            self.token_times.popleft()

    def _get_current_usage(self) -> Tuple[int, int]:
        """Get current RPM and TPM usage in the last 60 seconds."""
        self._clean_old_entries()
        rpm_used = len(self.request_times)
        tpm_used = sum(tokens for _, tokens in self.token_times)
        return rpm_used, tpm_used

    async def acquire(self, estimated_tokens: int):
        """
        Wait until we can make a request without exceeding rate limits.

        Args:
            estimated_tokens: Estimated tokens for the upcoming request
        """
        async with self.lock:
            while True:
                rpm_used, tpm_used = self._get_current_usage()

                # Check if we can make this request
                if (rpm_used < self.rpm_limit and
                        tpm_used + estimated_tokens < self.tpm_limit):
                    # Record this request
                    now = time.time()
                    self.request_times.append(now)
                    self.token_times.append((now, estimated_tokens))

                    self.total_requests += 1
                    self.total_tokens += estimated_tokens
                    return

                # Calculate wait time
                wait_time = 1.0

                if rpm_used >= self.rpm_limit and self.request_times:
                    oldest = self.request_times[0]
                    wait_time = max(wait_time, 60 - (time.time() - oldest) + 0.2)

                if tpm_used + estimated_tokens >= self.tpm_limit and self.token_times:
                    oldest_token_time = self.token_times[0][0]
                    wait_time = max(wait_time, 60 - (time.time() - oldest_token_time) + 0.2)

                print(f"⏳ Rate limit: RPM {rpm_used}/{self.rpm_limit}, "
                      f"TPM {tpm_used:,}/{self.tpm_limit:,}. "
                      f"Waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

    def get_stats(self) -> dict:
        """Get usage statistics."""
        rpm_used, tpm_used = self._get_current_usage()
        return {
            'total_requests': self.total_requests,
            'total_tokens': self.total_tokens,
            'current_rpm': rpm_used,
            'current_tpm': tpm_used,
            'rpm_limit': self.rpm_limit,
            'tpm_limit': self.tpm_limit
        }
