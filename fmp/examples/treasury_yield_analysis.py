"""
Treasury Yield Curve Analysis (2023-2024)

Analyzes:
1. 2Y-10Y spread evolution and curve inversion
2. Peak Fed Funds proxy (using 3-month rate)
3. Yield curve shape comparison: start vs end of period
"""


from fmp import FMPClient
import pandas as pd
import numpy as np

# Initialize FMP client
fmp = FMPClient()

# Fetch Treasury rates from 2023-01-01 to 2024-12-31
print("Fetching Treasury rates from 2023-01-01 to 2024-12-31...")
print("=" * 60)

treasury_df = fmp.fetch(
    "treasury_rates",
    **{"from": "2023-01-01", "to": "2024-12-31"}
)

# Convert date column and sort
treasury_df['date'] = pd.to_datetime(treasury_df['date'])
treasury_df = treasury_df.sort_values('date').reset_index(drop=True)

print(f"\nData retrieved: {len(treasury_df)} observations")
print(f"Date range: {treasury_df['date'].min().date()} to {treasury_df['date'].max().date()}")
print(f"\nColumns available: {list(treasury_df.columns)}")

# Display sample of the data
print("\nSample data (first 5 rows):")
print(treasury_df.head())

# ============================================================================
# 1. 2Y-10Y Spread Analysis
# ============================================================================
print("\n" + "=" * 60)
print("1. 2Y-10Y SPREAD ANALYSIS")
print("=" * 60)

# Calculate 2Y-10Y spread (negative = inverted)
treasury_df['spread_2y10y'] = treasury_df['year10'] - treasury_df['year2']

# Summary statistics
spread_mean = treasury_df['spread_2y10y'].mean()
spread_min = treasury_df['spread_2y10y'].min()
spread_max = treasury_df['spread_2y10y'].max()
spread_start = treasury_df.iloc[0]['spread_2y10y']
spread_end = treasury_df.iloc[-1]['spread_2y10y']

# Count inverted days
inverted_days = (treasury_df['spread_2y10y'] < 0).sum()
total_days = len(treasury_df)
pct_inverted = inverted_days / total_days * 100

print(f"\nSpread Statistics:")
print(f"  Mean spread:          {spread_mean:+.2f}%")
print(f"  Min spread (deepest): {spread_min:+.2f}%")
print(f"  Max spread:           {spread_max:+.2f}%")
print(f"  Start of period:      {spread_start:+.2f}%")
print(f"  End of period:        {spread_end:+.2f}%")

print(f"\nYield Curve Inversion:")
print(f"  Days with inverted curve (negative spread): {inverted_days} out of {total_days}")
print(f"  Percentage of time inverted: {pct_inverted:.1f}%")

if spread_min < 0:
    most_inverted = treasury_df.loc[treasury_df['spread_2y10y'].idxmin()]
    print(f"  Most inverted date: {most_inverted['date'].date()} at {most_inverted['spread_2y10y']:+.2f}%")

# Monthly spread evolution
treasury_df['month'] = treasury_df['date'].dt.to_period('M')
monthly_spread = treasury_df.groupby('month')['spread_2y10y'].mean()

print("\nMonthly Average 2Y-10Y Spread:")
print("-" * 40)
for period, spread in monthly_spread.items():
    status = "INVERTED" if spread < 0 else "normal"
    print(f"  {period}: {spread:+.2f}% ({status})")

# ============================================================================
# 2. Fed Funds Proxy (3-Month Treasury)
# ============================================================================
print("\n" + "=" * 60)
print("2. FED FUNDS PROXY (3-MONTH TREASURY)")
print("=" * 60)

# Using month3 as Fed Funds proxy
month3_max = treasury_df['month3'].max()
month3_min = treasury_df['month3'].min()
month3_start = treasury_df.iloc[0]['month3']
month3_end = treasury_df.iloc[-1]['month3']

peak_idx = treasury_df['month3'].idxmax()
peak_date = treasury_df.loc[peak_idx, 'date']

print(f"\n3-Month Treasury Rate (Fed Funds Proxy):")
print(f"  Start of period (Jan 2023): {month3_start:.2f}%")
print(f"  Peak rate:                  {month3_max:.2f}%")
print(f"  Peak date:                  {peak_date.date()}")
print(f"  End of period (Dec 2024):   {month3_end:.2f}%")
print(f"  Minimum during period:      {month3_min:.2f}%")
print(f"  Change from peak to end:    {month3_end - month3_max:+.2f}% (rate cut indicator)")

# Quarterly average
treasury_df['quarter'] = treasury_df['date'].dt.to_period('Q')
quarterly_month3 = treasury_df.groupby('quarter')['month3'].mean()

print("\nQuarterly Average 3-Month Rate:")
print("-" * 40)
for period, rate in quarterly_month3.items():
    print(f"  {period}: {rate:.2f}%")

# ============================================================================
# 3. Yield Curve Shape: Start vs End of Period
# ============================================================================
print("\n" + "=" * 60)
print("3. YIELD CURVE SHAPE: START vs END OF PERIOD")
print("=" * 60)

# Get first and last available dates
start_row = treasury_df.iloc[0]
end_row = treasury_df.iloc[-1]

# Define maturities and their column names
maturities = {
    '1M': 'month1',
    '3M': 'month3',
    '6M': 'month6',
    '1Y': 'year1',
    '2Y': 'year2',
    '5Y': 'year5',
    '10Y': 'year10',
    '30Y': 'year30'
}

print(f"\nYield Curve on {start_row['date'].date()} (Start):")
print("-" * 40)
start_curve = []
for label, col in maturities.items():
    if col in start_row and pd.notna(start_row[col]):
        rate = start_row[col]
        start_curve.append((label, rate))
        print(f"  {label:>4}: {rate:.2f}%")

print(f"\nYield Curve on {end_row['date'].date()} (End):")
print("-" * 40)
end_curve = []
for label, col in maturities.items():
    if col in end_row and pd.notna(end_row[col]):
        rate = end_row[col]
        end_curve.append((label, rate))
        print(f"  {label:>4}: {rate:.2f}%")

print("\nChange in Yields (End - Start):")
print("-" * 40)
for label, col in maturities.items():
    if col in start_row and col in end_row:
        if pd.notna(start_row[col]) and pd.notna(end_row[col]):
            change = end_row[col] - start_row[col]
            print(f"  {label:>4}: {change:+.2f}%")

# Curve shape analysis
print("\nCurve Shape Analysis:")
print("-" * 40)

# Start curve shape
if 'year2' in start_row and 'year10' in start_row:
    start_2y = start_row['year2']
    start_10y = start_row['year10']
    start_spread = start_10y - start_2y
    start_shape = "INVERTED" if start_spread < 0 else "NORMAL (upward sloping)"
    print(f"  Start ({start_row['date'].date()}):")
    print(f"    2Y: {start_2y:.2f}%, 10Y: {start_10y:.2f}%")
    print(f"    2Y-10Y Spread: {start_spread:+.2f}% - {start_shape}")

# End curve shape
if 'year2' in end_row and 'year10' in end_row:
    end_2y = end_row['year2']
    end_10y = end_row['year10']
    end_spread = end_10y - end_2y
    end_shape = "INVERTED" if end_spread < 0 else "NORMAL (upward sloping)"
    print(f"\n  End ({end_row['date'].date()}):")
    print(f"    2Y: {end_2y:.2f}%, 10Y: {end_10y:.2f}%")
    print(f"    2Y-10Y Spread: {end_spread:+.2f}% - {end_shape}")

# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)

print(f"""
Key Findings (2023-2024):

1. YIELD CURVE INVERSION:
   - The curve was inverted {pct_inverted:.1f}% of the time ({inverted_days}/{total_days} days)
   - Deepest inversion: {spread_min:+.2f}% on {most_inverted['date'].date() if spread_min < 0 else 'N/A'}
   - The 2Y-10Y spread moved from {spread_start:+.2f}% to {spread_end:+.2f}%

2. FED POLICY (via 3-Month Treasury proxy):
   - Peak rate of {month3_max:.2f}% reached on {peak_date.date()}
   - This reflects the Fed's tightening cycle reaching its peak
   - By end of 2024, rates had declined to {month3_end:.2f}% (change: {month3_end - month3_max:+.2f}%)

3. CURVE SHAPE EVOLUTION:
   - Start: {'Inverted' if start_spread < 0 else 'Normal'} curve (2Y-10Y: {start_spread:+.2f}%)
   - End: {'Inverted' if end_spread < 0 else 'Normal'} curve (2Y-10Y: {end_spread:+.2f}%)
   - The curve {'remained inverted' if end_spread < 0 and start_spread < 0 else 'normalized' if end_spread >= 0 and start_spread < 0 else 'stayed normal' if end_spread >= 0 and start_spread >= 0 else 'inverted'} over this period.
""")
