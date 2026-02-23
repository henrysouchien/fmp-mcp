"""Dividend Analysis for Classic Dividend Aristocrats (JNJ, PG, KO)."""

import pandas as pd
from fmp import FMPClient

# Initialize the FMP client
fmp = FMPClient()

# Define the dividend aristocrats to analyze
symbols = ["JNJ", "PG", "KO"]

print("=" * 70)
print("DIVIDEND ANALYSIS FOR CLASSIC DIVIDEND ARISTOCRATS")
print("=" * 70)

# Collect data for each symbol
dividend_data = {}
profile_data = {}

for symbol in symbols:
    print(f"\nFetching data for {symbol}...")

    # Fetch dividend history
    try:
        dividends_df = fmp.fetch("dividends", symbol=symbol)
        dividend_data[symbol] = dividends_df
        print(f"  - Got {len(dividends_df)} dividend records")
    except Exception as e:
        print(f"  - Error fetching dividends: {e}")
        dividend_data[symbol] = None

    # Fetch profile for current price
    try:
        profile_df = fmp.fetch("profile", symbol=symbol)
        profile_data[symbol] = profile_df
        print(f"  - Got company profile")
    except Exception as e:
        print(f"  - Error fetching profile: {e}")
        profile_data[symbol] = None

# ============================================================================
# 1. Current Dividend Amount for Each
# ============================================================================
print("\n" + "=" * 70)
print("1. CURRENT DIVIDEND AMOUNTS")
print("=" * 70)

for symbol in symbols:
    df = dividend_data.get(symbol)
    if df is not None and len(df) > 0:
        # Sort by date to get most recent first
        if 'date' in df.columns:
            df = df.sort_values('date', ascending=False)

        # Get the most recent dividend
        latest = df.iloc[0]
        dividend_amount = latest.get('dividend', latest.get('adjDividend', 'N/A'))
        payment_date = latest.get('paymentDate', latest.get('date', 'N/A'))

        print(f"\n{symbol}:")
        print(f"  Current dividend per share: ${dividend_amount:.4f}" if isinstance(dividend_amount, (int, float)) else f"  Current dividend per share: {dividend_amount}")
        print(f"  Most recent payment date: {payment_date}")
    else:
        print(f"\n{symbol}: No dividend data available")

# ============================================================================
# 2. Dividend Payment Frequency
# ============================================================================
print("\n" + "=" * 70)
print("2. DIVIDEND PAYMENT FREQUENCY")
print("=" * 70)

for symbol in symbols:
    df = dividend_data.get(symbol)
    if df is not None and len(df) > 0:
        # Convert dates and analyze frequency
        if 'date' in df.columns:
            df['date_parsed'] = pd.to_datetime(df['date'])
            df = df.sort_values('date_parsed', ascending=False)

            # Look at the last 2 years of data to determine frequency
            recent_year = df['date_parsed'].max().year
            year_data = df[df['date_parsed'].dt.year >= recent_year - 1]
            payments_last_year = len(df[df['date_parsed'].dt.year == recent_year])
            payments_prev_year = len(df[df['date_parsed'].dt.year == recent_year - 1])

            # Determine frequency based on number of payments
            avg_payments = (payments_last_year + payments_prev_year) / 2
            if avg_payments >= 11:
                frequency = "Monthly"
            elif avg_payments >= 3.5:
                frequency = "Quarterly"
            elif avg_payments >= 1.5:
                frequency = "Semi-Annual"
            else:
                frequency = "Annual"

            print(f"\n{symbol}:")
            print(f"  Payment frequency: {frequency}")
            print(f"  Payments in {recent_year}: {payments_last_year}")
            print(f"  Payments in {recent_year - 1}: {payments_prev_year}")
    else:
        print(f"\n{symbol}: No dividend data available")

# ============================================================================
# 3. Dividend Growth Analysis
# ============================================================================
print("\n" + "=" * 70)
print("3. DIVIDEND GROWTH ANALYSIS")
print("=" * 70)

current_year = pd.Timestamp.now().year

for symbol in symbols:
    df = dividend_data.get(symbol)
    if df is not None and len(df) > 0:
        if 'date' in df.columns:
            df['date_parsed'] = pd.to_datetime(df['date'])
            df = df.sort_values('date_parsed', ascending=False)

            # Get dividend column name
            div_col = 'dividend' if 'dividend' in df.columns else 'adjDividend'

            # Calculate annual dividends by summing quarterly payments
            df['year'] = df['date_parsed'].dt.year
            annual_dividends = df.groupby('year')[div_col].sum().sort_index()

            print(f"\n{symbol}:")
            print(f"  Annual dividend totals (complete years only):")

            # Only show complete years (exclude current year since it's partial)
            complete_years = [y for y in annual_dividends.index if y < current_year]
            for year in sorted(complete_years, reverse=True)[:5]:
                print(f"    {year}: ${annual_dividends[year]:.4f}")

            # Also show per-share dividend for most recent payments
            print(f"\n  Recent quarterly dividends:")
            for i, row in df.head(4).iterrows():
                date = row['date']
                div = row[div_col]
                print(f"    {date}: ${div:.4f}")

            # Calculate growth using complete years only
            if len(complete_years) >= 2:
                years = sorted(complete_years)
                if len(years) >= 5:
                    recent = annual_dividends[years[-1]]
                    older = annual_dividends[years[-5]]
                    cagr_5yr = ((recent / older) ** (1/4) - 1) * 100
                    print(f"\n  5-Year Dividend CAGR: {cagr_5yr:.2f}%")

                if len(years) >= 2:
                    # Year-over-year growth using last two complete years
                    recent_year = years[-1]
                    prev_year = years[-2]
                    yoy_growth = ((annual_dividends[recent_year] / annual_dividends[prev_year]) - 1) * 100
                    print(f"  YoY Dividend Growth ({prev_year} to {recent_year}): {yoy_growth:.2f}%")

                # Check if dividend has been growing (look at last 5 complete years)
                check_years = years[-5:] if len(years) >= 5 else years
                growing = True
                for i in range(len(check_years) - 1):
                    if annual_dividends[check_years[i+1]] < annual_dividends[check_years[i]]:
                        growing = False
                        break

                status = "YES - Consistently growing" if growing else "MIXED - Some years may have declined"
                print(f"  Has dividend been growing? {status}")
    else:
        print(f"\n{symbol}: No dividend data available")

# ============================================================================
# 4. Current Yield Comparison
# ============================================================================
print("\n" + "=" * 70)
print("4. CURRENT DIVIDEND YIELD COMPARISON")
print("=" * 70)

yield_comparison = []

for symbol in symbols:
    div_df = dividend_data.get(symbol)
    prof_df = profile_data.get(symbol)

    if div_df is not None and prof_df is not None and len(div_df) > 0 and len(prof_df) > 0:
        # Get current price from profile
        profile = prof_df.iloc[0]
        current_price = profile.get('price', None)
        company_name = profile.get('companyName', symbol)

        # Calculate trailing 12-month dividend
        if 'date' in div_df.columns:
            div_df['date_parsed'] = pd.to_datetime(div_df['date'])
            div_df = div_df.sort_values('date_parsed', ascending=False)

            # Get last 12 months of dividends
            cutoff_date = div_df['date_parsed'].max() - pd.DateOffset(months=12)
            ttm_dividends = div_df[div_df['date_parsed'] > cutoff_date]

            div_col = 'dividend' if 'dividend' in div_df.columns else 'adjDividend'
            annual_dividend = ttm_dividends[div_col].sum()

            if current_price and current_price > 0:
                dividend_yield = (annual_dividend / current_price) * 100

                yield_comparison.append({
                    'Symbol': symbol,
                    'Company': company_name,
                    'Price': current_price,
                    'TTM Dividend': annual_dividend,
                    'Yield (%)': dividend_yield
                })

                print(f"\n{symbol} ({company_name}):")
                print(f"  Current Price: ${current_price:.2f}")
                print(f"  TTM Dividend: ${annual_dividend:.4f}")
                print(f"  Dividend Yield: {dividend_yield:.2f}%")
    else:
        print(f"\n{symbol}: Missing data for yield calculation")

# Final Summary
if yield_comparison:
    print("\n" + "=" * 70)
    print("SUMMARY: HIGHEST YIELD WINNER")
    print("=" * 70)

    yield_df = pd.DataFrame(yield_comparison)
    yield_df = yield_df.sort_values('Yield (%)', ascending=False)

    print("\nRanking by Dividend Yield:")
    for i, row in yield_df.iterrows():
        rank = yield_df.index.get_loc(i) + 1
        print(f"  {rank}. {row['Symbol']}: {row['Yield (%)']:.2f}%")

    winner = yield_df.iloc[0]
    print(f"\nHighest Current Yield: {winner['Symbol']} at {winner['Yield (%)']:.2f}%")

print("\n" + "=" * 70)
print("Analysis Complete")
print("=" * 70)
