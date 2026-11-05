"""Token-aware rate limiter shared by every Gemini pipeline stage.

Two problems this module solves:

* **A fresh window per process.** ``run_pipeline`` runs each stage as its own
  subprocess, so a brand-new ``RateLimiter`` used to start with an empty 60 s
  window and could fire a full minute's budget the instant the previous stage
  exited — the categorizer's burst was still counted by Gemini, so the feature
  extractor immediately got 429s. The rolling window is now shared between
  stages: :func:`get_rate_limiter` caches one instance per ``(scope, rpm, tpm)``
  in the process, and the window is persisted to
  ``backend/data/logs/rate-limiter/<scope>.json`` so the *next* process resumes
  it. The state is best-effort and last-writer-wins, which is fine for the
  sequential pipeline; two simultaneous stage runs may under-count.

* **A budget that disagrees with the account.** A 429 response carries the real
  ``quotaValue`` for the exceeded metric; :func:`call_with_retries` feeds that
  back into the limiter, so a run configured for a paid tier self-corrects to a
  free-tier key instead of retry-storming the endpoint.
"""

import asyncio
import atexit
import json
import os
import re
import time
from collections import deque
from pathlib import Path
from typing import Callable, Optional, Tuple

WINDOW_SECONDS = 60

# Budget shared by the two stages that call the same text model. Keeping these
# in one place (and one ``scope``) is what lets extract_features inherit the
# categorizer's rolling window instead of starting from zero.
TEXT_MODEL_RPM_LIMIT = int(os.getenv("GEMINI_RPM_LIMIT", "3900"))
TEXT_MODEL_TPM_LIMIT = int(os.getenv("GEMINI_TPM_LIMIT", "3900000"))
TEXT_MODEL_SCOPE = "gemini-text"

_RETRYABLE_CODES = {429, 500, 502, 503, 504}
_RETRYABLE_STATUSES = {
    "RESOURCE_EXHAUSTED",
    "UNAVAILABLE",
    "INTERNAL",
    "DEADLINE_EXCEEDED",
}
_STATE_DIR = Path(__file__).resolve().parent / "logs" / "rate-limiter"
_PERSIST_INTERVAL = 1.0  # seconds between debounced state writes

_instances: dict = {}


def get_rate_limiter(
    rpm_limit: int = 3900,
    tpm_limit: int = 3_800_000,
    scope: Optional[str] = None,
    state_file: Optional[Path] = None,
) -> "RateLimiter":
    """Return the process-wide limiter for this budget + scope.

    The same ``scope`` in a later process resumes the persisted window, which is
    how separate pipeline stages share one rolling minute.
    """
    scope = scope or f"{rpm_limit}-{tpm_limit}"
    key = (scope, rpm_limit, tpm_limit)
    if key not in _instances:
        _instances[key] = RateLimiter(
            rpm_limit=rpm_limit,
            tpm_limit=tpm_limit,
            scope=scope,
            state_file=state_file,
        )
    return _instances[key]


class RateLimiter:
    """
    Token-aware rate limiter for API calls.
    Tracks requests per minute (RPM) and tokens per minute (TPM).
    """

    def __init__(
        self,
        rpm_limit: int = 3900,
        tpm_limit: int = 3800000,
        scope: Optional[str] = None,
        state_file: Optional[Path] = None,
    ):
        """
        Initialize rate limiter with conservative limits.

        Gemini 2.0 Flash tier 1:
        - 2,000 RPM
        - 4,000,000 TPM

        We use 3900/3.8m to leave a safety buffer. Override with the
        ``GEMINI_RPM_LIMIT`` / ``GEMINI_TPM_LIMIT`` environment variables: the
        budget must match the API key's tier, or the limiter lets through more
        traffic than the server does.
        """
        self.rpm_limit = rpm_limit
        self.tpm_limit = tpm_limit
        self.scope = scope or f"{rpm_limit}-{tpm_limit}"

        self.request_times = deque()  # Timestamps of requests
        self.token_times = deque()  # (timestamp, token_count) tuples

        self.lock = asyncio.Lock()
        self.total_requests = 0
        self.total_tokens = 0

        self.state_file = Path(state_file) if state_file else _STATE_DIR / f"{self.scope}.json"
        self._last_persist = 0.0
        self._load_state()
        atexit.register(self.flush)

    # -- persisted rolling window ------------------------------------------ #

    def _load_state(self) -> None:
        """Resume the window left by the previous stage process."""
        try:
            payload = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return

        cutoff = time.time() - WINDOW_SECONDS
        requests = [float(t) for t in payload.get("requests", []) if float(t) > cutoff]
        tokens = [
            (float(stamp), int(count))
            for stamp, count in payload.get("tokens", [])
            if float(stamp) > cutoff
        ]
        self.request_times.extend(requests)
        self.token_times.extend(tokens)
        if requests:
            print(
                f"[rate-limiter] '{self.scope}': resumed {len(requests)} requests "
                f"from the previous {WINDOW_SECONDS}s window"
            )

    def _persist(self, force: bool = False) -> None:
        """Write the window to disk so the next process can resume it.

        Debounced because the payload can hold a full minute of requests.
        """
        now = time.time()
        if not force and now - self._last_persist < _PERSIST_INTERVAL:
            return
        self._last_persist = now
        self._clean_old_entries()
        payload = {
            "scope": self.scope,
            "rpm_limit": self.rpm_limit,
            "tpm_limit": self.tpm_limit,
            "saved_at": now,
            "requests": list(self.request_times),
            "tokens": [[stamp, count] for stamp, count in self.token_times],
        }
        tmp = self.state_file.with_suffix(f".{os.getpid()}.tmp")
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp, self.state_file)
        except OSError as exc:
            print(f"[rate-limiter] WARNING: could not persist rate-limiter state to {self.state_file}: {exc}")

    def flush(self) -> None:
        """Force-write the window (used on interpreter exit)."""
        self._persist(force=True)

    # -- budget ------------------------------------------------------------- #

    def apply_quota_hint(self, kind: str, value: int) -> None:
        """Lower the budget to the value the server reported in a 429."""
        if value <= 0:
            return
        if kind == "tokens" and value < self.tpm_limit:
            print(f"[rate-limiter] '{self.scope}' TPM budget {self.tpm_limit:,} -> {value:,} (server quota)")
            self.tpm_limit = value
        elif kind == "requests" and value < self.rpm_limit:
            print(f"[rate-limiter] '{self.scope}' RPM budget {self.rpm_limit:,} -> {value:,} (server quota)")
            self.rpm_limit = value

    # -- window ------------------------------------------------------------- #

    def _clean_old_entries(self):
        """Remove entries older than 60 seconds."""
        cutoff = time.time() - WINDOW_SECONDS

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
                    self._persist()
                    return

                # Calculate wait time
                wait_time = 1.0

                if rpm_used >= self.rpm_limit and self.request_times:
                    oldest = self.request_times[0]
                    wait_time = max(wait_time, WINDOW_SECONDS - (time.time() - oldest) + 0.2)

                if tpm_used + estimated_tokens >= self.tpm_limit and self.token_times:
                    oldest_token_time = self.token_times[0][0]
                    wait_time = max(wait_time, WINDOW_SECONDS - (time.time() - oldest_token_time) + 0.2)

                print(f"[rate-limiter] waiting: RPM {rpm_used}/{self.rpm_limit}, "
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


# --------------------------------------------------------------------------- #
# Retry helpers for the Gemini SDK
# --------------------------------------------------------------------------- #

def _error_payload(exc: BaseException) -> dict:
    """Best-effort extraction of the ``error`` object from a google-genai error."""
    details = getattr(exc, "details", None)
    if isinstance(details, dict):
        error = details.get("error")
        return error if isinstance(error, dict) else details
    response = getattr(exc, "response", None)
    if response is not None and hasattr(response, "json"):
        try:
            payload = response.json()
        except Exception:  # noqa: BLE001 - malformed body, fall through
            payload = None
        if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
            return payload["error"]
    return {}


def is_retryable(exc: BaseException) -> bool:
    """True for quota exhaustion and transient server errors."""
    code = getattr(exc, "code", None)
    try:
        if int(code) in _RETRYABLE_CODES:
            return True
    except (TypeError, ValueError):
        pass
    status = str(getattr(exc, "status", "") or "").upper()
    if status in _RETRYABLE_STATUSES:
        return True
    text = str(exc).upper()
    return any(token in text for token in ("RESOURCE_EXHAUSTED", "UNAVAILABLE", "429", "503"))


def retry_after_seconds(exc: BaseException) -> Optional[float]:
    """Server-suggested wait (``RetryInfo.retryDelay``) in seconds, if present."""
    details = _error_payload(exc).get("details") or []
    for entry in details:
        if not isinstance(entry, dict):
            continue
        raw = entry.get("retryDelay")
        if raw:
            match = re.match(r"([\d.]+)s", str(raw))
            if match:
                return float(match.group(1))
    match = re.search(r"retry in ([\d.]+)s", str(exc), re.IGNORECASE)
    return float(match.group(1)) if match else None


def quota_hint(exc: BaseException) -> Optional[Tuple[str, int]]:
    """The per-minute quota the server says we exceeded: ``("requests"|"tokens", n)``."""
    details = _error_payload(exc).get("details") or []
    for entry in details:
        if not isinstance(entry, dict):
            continue
        for violation in entry.get("violations") or []:
            if not isinstance(violation, dict):
                continue
            metric = " ".join(
                str(violation.get(field, ""))
                for field in ("quotaId", "quotaMetric", "quotaDimensions")
            ).lower()
            if "minute" not in metric and "per_minute" not in metric:
                continue  # daily quotas must not be mistaken for an RPM cap
            try:
                value = int(violation.get("quotaValue"))
            except (TypeError, ValueError):
                continue
            kind = "tokens" if "token" in metric else "requests"
            return kind, value
    return None


async def call_with_retries(
    request: Callable[[], "asyncio.Future"],
    *,
    rate_limiter: RateLimiter,
    estimated_tokens: int,
    attempts: int = 5,
    label: str = "gemini",
):
    """Rate-limit and retry one Gemini call.

    ``request`` must be a zero-argument callable that starts a fresh coroutine
    (a lambda around ``asyncio.to_thread(...)``), because a coroutine object
    cannot be awaited twice. Every attempt goes through ``rate_limiter`` so the
    retry itself is budgeted.
    """
    for attempt in range(1, attempts + 1):
        await rate_limiter.acquire(estimated_tokens)
        try:
            return await request()
        except Exception as exc:  # noqa: BLE001 - classified below
            if not is_retryable(exc) or attempt == attempts:
                raise
            hint = quota_hint(exc)
            if hint:
                rate_limiter.apply_quota_hint(*hint)
            delay = retry_after_seconds(exc)
            if delay is None:
                delay = min(2 ** attempt, WINDOW_SECONDS)
            print(
                f"   [{label}] retry {attempt}/{attempts - 1} in {delay:.1f}s "
                f"({type(exc).__name__})"
            )
            await asyncio.sleep(delay)
