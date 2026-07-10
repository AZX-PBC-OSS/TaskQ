"""Credential-safe DSN host extraction for logging.

Single-purpose tiny module — same pattern as :mod:`taskq._json`.
The leading underscore on the module name signals "internal to taskq."
"""

from urllib.parse import urlparse

__all__ = ["dsn_host"]


def dsn_host(dsn: object) -> str:
    """Extract host from a DSN for safe logging (no credentials)."""
    try:
        parsed = urlparse(str(dsn))
        return parsed.hostname or "unknown"
    except Exception:
        return "unknown"
