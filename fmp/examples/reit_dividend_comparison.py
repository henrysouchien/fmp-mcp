"""
REIT Dividend Comparison Script

Compares dividend characteristics of three REITs:
- O (Realty Income) - Monthly dividend
- STWD (Starwood Property) - Quarterly dividend
- NLY (Annaly Capital) - Quarterly dividend
"""

import pandas as pd
from fmp import FMPClient

# Initialize client
fmp = FMPClient()

# REITs to compare
reits = ["O", "STWD", "NLY"]

print("=" * 70)
print("REIT DIVIDEND COMPARISON")
print("=" * 70)

# Fetch data for each REIT
profiles = {}
dividends = {}
key_metrics = {}

for symbol in reits:
    print(f"\nFetching data for {symbol}...")

    # Get company profile
    try:
        profile_df = fmp.fetch("profile", symbol=symbol)
        profiles[symbol] = profile_df.iloc[0] if not profile_df.empty else None
    except Exception as e:
        print(f"  Error fetching profile: {e}")
        profiles[symbol] = None

    # Get dividend history
    try:
        div_df = fmp.fetch("dividends", symbol=symbol)
        dividends[symbol] = div_df
    except Exception as e:
        print(f"  Error fetching dividends: {e}")
        dividends[symbol] = pd.DataFrame()

    # Get key metrics
    try:
        metrics_df = fmp.fetch("key_metrics", symbol=symbol, period="quarter", limit=4)
        key_metrics[symbol] = metrics_df
    except Exception as e:
        print(f"  Error fetching key metrics: {e}")
        key_metrics[symbol] = pd.DataFrame()

# ============================================================================
# 1. Company Overview
# ============================================================================
print("\n" + "=" * 70)
print("1. COMPANY OVERVIEW")
print("=" * 70)

for symbol in reits:
    profile = profiles.get(symbol)
    if profile is not None:
        print(f"\n{symbol}: {profile.get('companyName', 'N/A')}")
        print(f"  Sector: {profile.get('sector', 'N/A')}")
        print(f"  Industry: {profile.get('industry', 'N/A')}")
        print(f"  Price: ${profile.get('price', 'N/A'):.2f}" if pd.notna(profile.get('price')) else "  Price: N/A")
        print(f"  Market Cap: ${profile.get('mktCap', 0) / 1e9:.2f}B" if pd.notna(profile.get('mktCap')) else "  Market Cap: N/A")
    else:
        print(f"\n{symbol}: Profile data unavailable")

# ============================================================================
# 2. Dividend Frequency Analysis
# ============================================================================
print("\n" + "=" * 70)
print("2. DIVIDEND FREQUENCY ANALYSIS")
print("=" * 70)

def analyze_dividend_frequency(div_df, symbol):
    """Analyze dividend payment frequency from dividend history."""
    if div_df.empty:
        return "Unknown", 0

    # Convert date column
    if 'date' in div_df.columns:
        div_df['date'] = pd.to_datetime(div_df['date'])
        div_df = div_df.sort_values('date', ascending=False)
    elif 'paymentDate' in div_df.columns:
        div_df['date'] = pd.to_datetime(div_df['paymentDate'])
        div_df = div_df.sort_values('date', ascending=False)

    # Look at recent year's dividends
    recent_divs = div_df[div_df['date'] >= (pd.Timestamp.now() - pd.Timedelta(days=365))]

    if recent_divs.empty:
        recent_divs = div_df.head(12)  # Fallback to most recent 12

    count = len(recent_divs)

    if count >= 11:
        return "Monthly", count
    elif count >= 3 and count <= 5:
        return "Quarterly", count
    elif count >= 1 and count <= 2:
        return "Semi-Annual/Annual", count
    else:
        return "Irregular", count

for symbol in reits:
    div_df = dividends.get(symbol, pd.DataFrame())
    freq, count = analyze_dividend_frequency(div_df, symbol)
    print(f"\n{symbol}:")
    print(f"  Payment Frequency: {freq}")
    print(f"  Payments in last year: {count}")

# ============================================================================
# 3. Recent Dividend Amounts
# ============================================================================
print("\n" + "=" * 70)
print("3. RECENT DIVIDEND HISTORY (Last 6 Payments)")
print("=" * 70)

for symbol in reits:
    div_df = dividends.get(symbol, pd.DataFrame())
    print(f"\n{symbol}:")

    if div_df.empty:
        print("  No dividend data available")
        continue

    # Get relevant columns
    if 'date' in div_df.columns or 'paymentDate' in div_df.columns:
        date_col = 'date' if 'date' in div_df.columns else 'paymentDate'
        div_df[date_col] = pd.to_datetime(div_df[date_col])
        div_df = div_df.sort_values(date_col, ascending=False)

    # Display recent dividends
    amount_col = 'dividend' if 'dividend' in div_df.columns else 'adjDividend' if 'adjDividend' in div_df.columns else None

    if amount_col:
        recent = div_df.head(6)
        for _, row in recent.iterrows():
            date_str = row[date_col].strftime('%Y-%m-%d') if pd.notna(row[date_col]) else 'N/A'
            amount = row[amount_col] if pd.notna(row[amount_col]) else 0
            print(f"  {date_str}: ${amount:.4f}")
    else:
        print(f"  Columns available: {div_df.columns.tolist()}")
        print(div_df.head(6).to_string())

# ============================================================================
# 4. Dividend Yield & Key Metrics
# ============================================================================
print("\n" + "=" * 70)
print("4. DIVIDEND YIELD & KEY METRICS")
print("=" * 70)

for symbol in reits:
    metrics_df = key_metrics.get(symbol, pd.DataFrame())
    profile = profiles.get(symbol)

    print(f"\n{symbol}:")

    # Calculate annualized dividend from history
    div_df = dividends.get(symbol, pd.DataFrame())
    if not div_df.empty:
        amount_col = 'dividend' if 'dividend' in div_df.columns else 'adjDividend' if 'adjDividend' in div_df.columns else None
        if amount_col and 'date' in div_df.columns:
            div_df['date'] = pd.to_datetime(div_df['date'])
            recent_year = div_df[div_df['date'] >= (pd.Timestamp.now() - pd.Timedelta(days=365))]
            annual_div = recent_year[amount_col].sum() if not recent_year.empty else 0

            price = profile.get('price') if profile is not None else None
            if price and price > 0 and annual_div > 0:
                div_yield = (annual_div / price) * 100
                print(f"  Annual Dividend (TTM): ${annual_div:.2f}")
                print(f"  Current Price: ${price:.2f}")
                print(f"  Dividend Yield: {div_yield:.2f}%")

    # Display key metrics from most recent quarter
    if not metrics_df.empty:
        latest = metrics_df.iloc[0]

        if 'dividendYield' in latest and pd.notna(latest['dividendYield']):
            print(f"  Reported Dividend Yield: {latest['dividendYield'] * 100:.2f}%")

        if 'payoutRatio' in latest and pd.notna(latest['payoutRatio']):
            print(f"  Payout Ratio: {latest['payoutRatio'] * 100:.2f}%")

        if 'priceToBookRatio' in latest and pd.notna(latest['priceToBookRatio']):
            print(f"  Price/Book: {latest['priceToBookRatio']:.2f}")

        if 'returnOnEquity' in latest and pd.notna(latest['returnOnEquity']):
            print(f"  Return on Equity: {latest['returnOnEquity'] * 100:.2f}%")

# ============================================================================
# 5. Summary Comparison Table
# ============================================================================
print("\n" + "=" * 70)
print("5. SUMMARY COMPARISON TABLE")
print("=" * 70)

summary_data = []
for symbol in reits:
    profile = profiles.get(symbol)
    div_df = dividends.get(symbol, pd.DataFrame())
    metrics_df = key_metrics.get(symbol, pd.DataFrame())

    freq, count = analyze_dividend_frequency(div_df, symbol)

    # Calculate yield
    div_yield = None
    if not div_df.empty:
        amount_col = 'dividend' if 'dividend' in div_df.columns else 'adjDividend' if 'adjDividend' in div_df.columns else None
        if amount_col and 'date' in div_df.columns:
            div_df['date'] = pd.to_datetime(div_df['date'])
            recent_year = div_df[div_df['date'] >= (pd.Timestamp.now() - pd.Timedelta(days=365))]
            annual_div = recent_year[amount_col].sum() if not recent_year.empty else 0
            price = profile.get('price') if profile is not None else None
            if price and price > 0 and annual_div > 0:
                div_yield = (annual_div / price) * 100

    summary_data.append({
        'Symbol': symbol,
        'Company': profile.get('companyName', 'N/A')[:30] if profile is not None else 'N/A',
        'Frequency': freq,
        'Payments/Yr': count,
        'Div Yield %': f"{div_yield:.2f}" if div_yield else 'N/A',
        'Price': f"${profile.get('price', 0):.2f}" if profile is not None and pd.notna(profile.get('price')) else 'N/A'
    })

summary_df = pd.DataFrame(summary_data)
print("\n" + summary_df.to_string(index=False))

print("\n" + "=" * 70)
print("Analysis Complete")
print("=" * 70)
