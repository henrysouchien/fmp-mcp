"""Test FMP company search and profile endpoints."""

from fmp import FMPClient

# Initialize the client
fmp = FMPClient()

# 1. Search for "artificial intelligence" companies
print("=" * 70)
print("Searching for 'artificial intelligence' companies...")
print("=" * 70)

search_results = fmp.fetch("search", query="artificial intelligence", limit=10)
print(f"\nFound {len(search_results)} results:\n")
print(search_results[["symbol", "name", "stockExchange"]].to_string())

# 2. Get profiles for top 3 results
print("\n" + "=" * 70)
print("Fetching profiles for top 3 companies...")
print("=" * 70)

top_3_symbols = search_results["symbol"].head(3).tolist()
profiles = []

for symbol in top_3_symbols:
    print(f"\nFetching profile for: {symbol}")
    profile_df = fmp.fetch("profile", symbol=symbol)
    profiles.append(profile_df.iloc[0])

# 3. Summarize the results
print("\n" + "=" * 70)
print("SUMMARY: Top 3 AI Company Profiles")
print("=" * 70)

for profile in profiles:
    print(f"\n{'─' * 60}")
    print(f"Company: {profile.get('companyName', 'N/A')}")
    print(f"Symbol:  {profile.get('symbol', 'N/A')}")
    print(f"Sector:  {profile.get('sector', 'N/A')}")
    print(f"Industry: {profile.get('industry', 'N/A')}")

    # Format market cap
    mkt_cap = profile.get('mktCap', 0)
    if mkt_cap and mkt_cap > 0:
        if mkt_cap >= 1e12:
            mkt_cap_str = f"${mkt_cap / 1e12:.2f} Trillion"
        elif mkt_cap >= 1e9:
            mkt_cap_str = f"${mkt_cap / 1e9:.2f} Billion"
        elif mkt_cap >= 1e6:
            mkt_cap_str = f"${mkt_cap / 1e6:.2f} Million"
        else:
            mkt_cap_str = f"${mkt_cap:,.0f}"
    else:
        mkt_cap_str = "N/A"
    print(f"Market Cap: {mkt_cap_str}")

    # Description (truncated)
    desc = profile.get('description', 'N/A')
    if desc and len(desc) > 300:
        desc = desc[:300] + "..."
    print(f"Description: {desc}")

print("\n" + "=" * 70)
print("Test complete!")
print("=" * 70)
