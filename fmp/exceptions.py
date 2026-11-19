"""FMP-specific exceptions.

This module defines exception classes for FMP API operations,
providing clear error categorization for different failure modes.
"""

from __future__ import annotations


class FMPError(Exception):
    """Base exception for all FMP-related errors."""

    pass


class FMPAPIError(FMPError):
    """Raised when the FMP API returns an error response."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        endpoint: str | None = None,
        *,
        content_type: str | None = None,
        body_excerpt: str | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint
        self.content_type = content_type
        self.body_excerpt = body_excerpt

class FMPRateLimitError(FMPAPIError):
    """Raised when FMP rate limit is exceeded (HTTP 429)."""

    def __init__(self, endpoint: str | None = None):
        super().__init__(
            "FMP API rate limit exceeded. Please wait before retrying.",
            status_code=429,
            endpoint=endpoint,
        )


class FMPTimeoutError(FMPAPIError):
    """The request to FMP timed out: no answer arrived. Waiting can clear it.

    A typed fact, not a message: a caller that must tell an outage from a refusal reads this class. The
    transport's own error is not chained (its text can carry the request's URL and key), so the type is
    the only carrier.
    """


class FMPConnectionError(FMPAPIError):
    """The request to FMP could not be made (the connection failed or was refused). Waiting can clear it.

    Typed for the same reason as ``FMPTimeoutError``; the transport's error is not chained.
    """


class FMPAuthenticationError(FMPAPIError):
    """Raised when FMP API key is invalid or missing."""

    def __init__(self, message: str = "Invalid or missing FMP API key."):
        super().__init__(message, status_code=401)


class FMPEndpointError(FMPError):
    """Raised when an endpoint is not found or misconfigured."""

    def __init__(self, endpoint_name: str):
        super().__init__(f"Unknown FMP endpoint: '{endpoint_name}'")
        self.endpoint_name = endpoint_name


class FMPValidationError(FMPError):
    """Raised when endpoint parameters fail validation."""

    def __init__(self, message: str, param_name: str | None = None):
        super().__init__(message)
        self.param_name = param_name


class FMPEmptyResponseError(FMPError):
    """Raised when FMP returns empty or unusable data."""

    def __init__(self, endpoint: str, symbol: str | None = None):
        msg = f"FMP returned empty data for endpoint '{endpoint}'"
        if symbol:
            msg += f" (symbol: {symbol})"
        super().__init__(msg)
        self.endpoint = endpoint
        self.symbol = symbol


class FMPDefinitionArtifactError(FMPError):
    """Raised when pinned FMP definition evidence fails closed."""
