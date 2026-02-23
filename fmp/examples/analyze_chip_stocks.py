"""
Analyze price performance for NVDA, AMD, and INTC from 2023-01-01 to 2024-12-31.

Calculates:
1. Total return for each stock
2. Max drawdown for each stock
3. Comparison of performance
"""

import pandas as pd
import numpy as np
from fmp import FMPClient

# Initialize client
fmp = FMPClient()

# Stocks to analyze
symbols = ["NVDA", "AMD", "INTC"]
start_date = "2023-01-01"
end_date = "2024-12-31"

# Fetch dividend-adjusted prices for each stock
print("Fetching dividend-adjusted price data...")
print("=" * 60)

stock_data = {}
for symbol in symbols:
    df = fmp.fetch(
        "historical_price_adjusted",
        symbol=symbol,
        **{"from": start_date, "to": end_date}
    )
    # Sort by date ascending
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    stock_data[symbol] = df
    print(f"{symbol}: {len(df)} trading days fetched")
    print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"  Columns: {list(df.columns)}")

print("\n" + "=" * 60)
print("PERFORMANCE ANALYSIS")
print("=" * 60)

# Calculate metrics for each stock
results = []

for symbol in symbols:
    df = stock_data[symbol]

    # Use adjClose for dividend-adjusted returns
    prices = df['adjClose'].values

    # Total return: (end_price / start_price) - 1
    start_price = prices[0]
    end_price = prices[-1]
    total_return = (end_price / start_price - 1) * 100

    # Max drawdown calculation
    # Cumulative maximum price up to each point
    cummax = np.maximum.accumulate(prices)
    # Drawdown at each point
    drawdowns = (prices - cummax) / cummax * 100
    max_drawdown = drawdowns.min()

    results.append({
        'Symbol': symbol,
        'Start Price': start_price,
        'End Price': end_price,
        'Total Return (%)': total_return,
        'Max Drawdown (%)': max_drawdown
    })

    print(f"\n{symbol}:")
    print(f"  Start Price (adj): ${start_price:.2f}")
    print(f"  End Price (adj):   ${end_price:.2f}")
    print(f"  Total Return:      {total_return:+.2f}%")
    print(f"  Max Drawdown:      {max_drawdown:.2f}%")

# Summary comparison
print("\n" + "=" * 60)
print("COMPARISON SUMMARY")
print("=" * 60)

results_df = pd.DataFrame(results)
results_df = results_df.sort_values('Total Return (%)', ascending=False)
print("\nRanked by Total Return:")
print(results_df.to_string(index=False))

# Identify best performer
best_performer = results_df.iloc[0]['Symbol']
best_return = results_df.iloc[0]['Total Return (%)']
best_drawdown = results_df.iloc[0]['Max Drawdown (%)']

print(f"\n{'=' * 60}")
print("CONCLUSION")
print("=" * 60)
print(f"\nBest Performer: {best_performer}")
print(f"  - Total Return: {best_return:+.2f}%")
print(f"  - Max Drawdown: {best_drawdown:.2f}%")

# Risk-adjusted comparison (simple return/drawdown ratio)
print("\nRisk-Adjusted Performance (Return / |Max Drawdown|):")
for _, row in results_df.iterrows():
    risk_adj = row['Total Return (%)'] / abs(row['Max Drawdown (%)'])
    print(f"  {row['Symbol']}: {risk_adj:.2f}")
