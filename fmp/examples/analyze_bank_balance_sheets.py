"""
Analyze balance sheet strength for major US banks (JPM, BAC, WFC).

Metrics analyzed:
1. Total assets and equity
2. Leverage ratio (assets / equity)
3. Cash and securities as % of assets
4. Overall capitalization assessment
"""

import pandas as pd
from fmp import FMPClient

# Initialize client
fmp = FMPClient()

# Banks to analyze
banks = ["JPM", "BAC", "WFC"]

# Fetch balance sheets (last 2 years, annual)
print("Fetching balance sheet data for major US banks...\n")
print("=" * 80)

all_data = {}
for symbol in banks:
    df = fmp.fetch("balance_sheet", symbol=symbol, period="annual", limit=2)
    all_data[symbol] = df
    print(f"\n{symbol} - Balance Sheet Data:")
    print(f"  Columns available: {list(df.columns)[:20]}...")  # Show first 20 columns
    print(f"  Periods: {df['date'].tolist() if 'date' in df.columns else 'N/A'}")

# Analyze each bank
print("\n" + "=" * 80)
print("\nBALANCE SHEET ANALYSIS")
print("=" * 80)

results = []

for symbol in banks:
    df = all_data[symbol]

    print(f"\n{'='*40}")
    print(f"  {symbol}")
    print(f"{'='*40}")

    for idx, row in df.iterrows():
        fiscal_year = row.get('calendarYear', row.get('date', 'N/A'))

        # Extract key values
        total_assets = row.get('totalAssets', 0)
        total_equity = row.get('totalStockholdersEquity', row.get('totalEquity', 0))

        # Cash and equivalents
        cash = row.get('cashAndCashEquivalents', 0)
        short_term_investments = row.get('shortTermInvestments', 0)

        # For banks, securities are important - check various columns
        long_term_investments = row.get('longTermInvestments', 0)

        # Calculate metrics
        leverage_ratio = total_assets / total_equity if total_equity > 0 else float('inf')
        cash_securities = cash + short_term_investments + long_term_investments
        cash_securities_pct = (cash_securities / total_assets * 100) if total_assets > 0 else 0

        # Equity ratio (inverse of leverage conceptually)
        equity_ratio = (total_equity / total_assets * 100) if total_assets > 0 else 0

        print(f"\n  Fiscal Year: {fiscal_year}")
        print(f"  ------------------------------------")
        print(f"  Total Assets:      ${total_assets/1e9:,.1f}B")
        print(f"  Total Equity:      ${total_equity/1e9:,.1f}B")
        print(f"  Leverage Ratio:    {leverage_ratio:.2f}x (Assets/Equity)")
        print(f"  Equity Ratio:      {equity_ratio:.2f}% (Equity/Assets)")
        print(f"  Cash & Equiv:      ${cash/1e9:,.1f}B")
        print(f"  Short-term Inv:    ${short_term_investments/1e9:,.1f}B")
        print(f"  Long-term Inv:     ${long_term_investments/1e9:,.1f}B")
        print(f"  Cash+Securities:   ${cash_securities/1e9:,.1f}B ({cash_securities_pct:.1f}% of assets)")

        results.append({
            'Symbol': symbol,
            'Year': fiscal_year,
            'Total Assets ($B)': total_assets / 1e9,
            'Total Equity ($B)': total_equity / 1e9,
            'Leverage Ratio': leverage_ratio,
            'Equity Ratio (%)': equity_ratio,
            'Cash+Securities ($B)': cash_securities / 1e9,
            'Cash+Securities (%)': cash_securities_pct
        })

# Summary comparison
print("\n" + "=" * 80)
print("\nSUMMARY COMPARISON (Most Recent Year)")
print("=" * 80)

results_df = pd.DataFrame(results)

# Get most recent year for each bank
latest = results_df.groupby('Symbol').first().reset_index()

print("\n" + latest.to_string(index=False))

# Analysis
print("\n" + "=" * 80)
print("\nCAPITALIZATION ANALYSIS")
print("=" * 80)

# Lower leverage = more conservative
latest_sorted_leverage = latest.sort_values('Leverage Ratio')
print(f"\nBy Leverage Ratio (lower = more conservative):")
for idx, row in latest_sorted_leverage.iterrows():
    print(f"  {row['Symbol']}: {row['Leverage Ratio']:.2f}x")

# Higher equity ratio = more conservative
latest_sorted_equity = latest.sort_values('Equity Ratio (%)', ascending=False)
print(f"\nBy Equity Ratio (higher = more conservative):")
for idx, row in latest_sorted_equity.iterrows():
    print(f"  {row['Symbol']}: {row['Equity Ratio (%)']:.2f}%")

# Higher cash/securities = more liquid
latest_sorted_liquid = latest.sort_values('Cash+Securities (%)', ascending=False)
print(f"\nBy Liquidity (Cash+Securities % of Assets, higher = more liquid):")
for idx, row in latest_sorted_liquid.iterrows():
    print(f"  {row['Symbol']}: {row['Cash+Securities (%)']:.1f}%")

# Determine most conservative
print("\n" + "=" * 80)
print("\nCONCLUSION: Which Bank Appears Most Conservatively Capitalized?")
print("=" * 80)

# Score each bank (lower leverage = better, higher equity = better, higher liquidity = better)
scores = {}
for symbol in banks:
    bank_data = latest[latest['Symbol'] == symbol].iloc[0]
    # Inverse of leverage for scoring (lower is better)
    leverage_score = 1 / bank_data['Leverage Ratio']
    equity_score = bank_data['Equity Ratio (%)']
    liquidity_score = bank_data['Cash+Securities (%)']

    # Normalized composite score (equal weighting)
    scores[symbol] = {
        'leverage_score': leverage_score,
        'equity_score': equity_score,
        'liquidity_score': liquidity_score
    }

# Rank by each metric
print("\nRanking Summary:")
print("-" * 60)

# Best by leverage
best_leverage = min(latest.to_dict('records'), key=lambda x: x['Leverage Ratio'])
print(f"  Lowest Leverage:      {best_leverage['Symbol']} ({best_leverage['Leverage Ratio']:.2f}x)")

# Best by equity ratio
best_equity = max(latest.to_dict('records'), key=lambda x: x['Equity Ratio (%)'])
print(f"  Highest Equity Ratio: {best_equity['Symbol']} ({best_equity['Equity Ratio (%)']:.2f}%)")

# Best by liquidity
best_liquid = max(latest.to_dict('records'), key=lambda x: x['Cash+Securities (%)'])
print(f"  Highest Liquidity:    {best_liquid['Symbol']} ({best_liquid['Cash+Securities (%)']:.1f}%)")

# Overall assessment
print("\n" + "-" * 60)
print("\nOverall Assessment:")
print("-" * 60)

# Count wins for each bank
wins = {bank: 0 for bank in banks}
if best_leverage['Symbol']:
    wins[best_leverage['Symbol']] += 1
if best_equity['Symbol']:
    wins[best_equity['Symbol']] += 1
if best_liquid['Symbol']:
    wins[best_liquid['Symbol']] += 1

for bank, win_count in sorted(wins.items(), key=lambda x: -x[1]):
    print(f"  {bank}: {win_count} metric(s) where most conservative")

most_conservative = max(wins.items(), key=lambda x: x[1])[0]
print(f"\n  => Based on the analysis, {most_conservative} appears to be the most conservatively capitalized bank.")
print(f"     (Won in {wins[most_conservative]} of 3 key metrics)")
