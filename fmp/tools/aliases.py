"""Alias helpers for MCP tool input matching."""

# Common shorthands users might use for brokerages.
_BROKERAGE_SHORTHANDS: dict[str, str] = {
    "ibkr": "interactive brokers",
    "bofa": "merrill",
    "ml": "merrill",
}


def match_brokerage(query: str, brokerage_name: str) -> bool:
    """Check if query matches brokerage_name (substring, alias-aware, case-insensitive)."""
    normalized_query = str(query or "").strip().lower()
    normalized_name = str(brokerage_name or "").strip().lower()

    if not normalized_query or not normalized_name:
        return False

    if normalized_query in normalized_name:
        return True

    expanded_query = _BROKERAGE_SHORTHANDS.get(normalized_query)
    if expanded_query and expanded_query in normalized_name:
        return True

    return False
