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
    ):
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint


class FMPRateLimitError(FMPAPIError):
    """Raised when FMP rate limit is exceeded (HTTP 429)."""

    def __init__(self, endpoint: str | None = None):
        super().__init__(
            "FMP API rate limit exceeded. Please wait before retrying.",
            status_code=429,
            endpoint=endpoint,
        )


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
