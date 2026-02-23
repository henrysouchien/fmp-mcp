#!/usr/bin/env python3
"""Balance Sheet Strength Analysis for AAPL and MSFT.

Compares total assets, equity, cash position, and debt levels
using the last 2 years of annual balance sheet data from FMP.
"""

import pandas as pd
from fmp import FMPClient

# Initialize client
fmp = FMPClient()

# Fetch balance sheets for both companies (last 2 years)
print("Fetching balance sheet data...")
print("-" * 60)

tickers = ["AAPL", "MSFT"]
balance_sheets = {}

for ticker in tickers:
    df = fmp.fetch("balance_sheet", symbol=ticker, period="annual", limit=2)
    balance_sheets[ticker] = df
    print(f"\n{ticker} - Available columns:")
    print(df.columns.tolist()[:20])  # Show first 20 columns

# Key metrics to analyze
metrics = [
    # Total Assets and Equity
    ("totalAssets", "Total Assets"),
    ("totalStockholdersEquity", "Total Equity"),
    # Cash Position
    ("cashAndCashEquivalents", "Cash & Cash Equivalents"),
    ("cashAndShortTermInvestments", "Cash + Short-term Investments"),
    # Debt Levels
    ("totalDebt", "Total Debt"),
    ("shortTermDebt", "Short-term Debt"),
    ("longTermDebt", "Long-term Debt"),
    ("netDebt", "Net Debt"),
]

print("\n" + "=" * 80)
print("BALANCE SHEET STRENGTH COMPARISON: AAPL vs MSFT")
print("(Last 2 Years - Annual Data)")
print("=" * 80)


def format_billions(value):
    """Format value in billions."""
    if pd.isna(value):
        return "N/A"
    return f"${value / 1e9:.1f}B"


# Display comparison for each metric category
for category, category_name in [
    (["totalAssets", "totalStockholdersEquity"], "1. TOTAL ASSETS & EQUITY"),
    (["cashAndCashEquivalents", "cashAndShortTermInvestments"], "2. CASH POSITION"),
    (["totalDebt", "shortTermDebt", "longTermDebt", "netDebt"], "3. DEBT LEVELS"),
]:
    print(f"\n{category_name}")
    print("-" * 70)

    for metric_key in category:
        metric_label = next(
            (label for key, label in metrics if key == metric_key), metric_key
        )
        print(f"\n  {metric_label}:")

        for ticker in tickers:
            df = balance_sheets[ticker]
            if metric_key in df.columns:
                for _, row in df.iterrows():
                    date = row.get("date", row.get("fiscalDateEnding", "Unknown"))
                    value = row[metric_key]
                    print(f"    {ticker} ({date}): {format_billions(value)}")
            else:
                print(f"    {ticker}: Column '{metric_key}' not found")

# Calculate and display key ratios
print("\n" + "=" * 80)
print("KEY FINANCIAL RATIOS (Most Recent Year)")
print("=" * 80)

print("\n{:<30} {:>20} {:>20}".format("Ratio", "AAPL", "MSFT"))
print("-" * 70)

for ticker in tickers:
    df = balance_sheets[ticker]
    latest = df.iloc[0]  # Most recent year

    if ticker == "AAPL":
        aapl_data = latest
    else:
        msft_data = latest

# Debt-to-Equity Ratio
aapl_de = aapl_data["totalDebt"] / aapl_data["totalStockholdersEquity"] if aapl_data["totalStockholdersEquity"] != 0 else float('inf')
msft_de = msft_data["totalDebt"] / msft_data["totalStockholdersEquity"] if msft_data["totalStockholdersEquity"] != 0 else float('inf')
print("{:<30} {:>20.2f} {:>20.2f}".format("Debt-to-Equity", aapl_de, msft_de))

# Cash Ratio (Cash / Total Liabilities)
aapl_liab = aapl_data.get("totalLiabilities", aapl_data["totalAssets"] - aapl_data["totalStockholdersEquity"])
msft_liab = msft_data.get("totalLiabilities", msft_data["totalAssets"] - msft_data["totalStockholdersEquity"])

aapl_cash_ratio = aapl_data["cashAndCashEquivalents"] / aapl_liab if aapl_liab != 0 else float('inf')
msft_cash_ratio = msft_data["cashAndCashEquivalents"] / msft_liab if msft_liab != 0 else float('inf')
print("{:<30} {:>20.2f} {:>20.2f}".format("Cash Ratio (Cash/Liabilities)", aapl_cash_ratio, msft_cash_ratio))

# Equity Ratio (Equity / Total Assets)
aapl_eq_ratio = aapl_data["totalStockholdersEquity"] / aapl_data["totalAssets"]
msft_eq_ratio = msft_data["totalStockholdersEquity"] / msft_data["totalAssets"]
print("{:<30} {:>20.2%} {:>20.2%}".format("Equity Ratio", aapl_eq_ratio, msft_eq_ratio))

# Net Debt / Total Assets
if "netDebt" in aapl_data and "netDebt" in msft_data:
    aapl_nd_ratio = aapl_data["netDebt"] / aapl_data["totalAssets"]
    msft_nd_ratio = msft_data["netDebt"] / msft_data["totalAssets"]
    print("{:<30} {:>20.2%} {:>20.2%}".format("Net Debt / Assets", aapl_nd_ratio, msft_nd_ratio))

# Summary comparison
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print("\nAPPLE (AAPL):")
print(f"  - Total Assets: {format_billions(aapl_data['totalAssets'])}")
print(f"  - Total Equity: {format_billions(aapl_data['totalStockholdersEquity'])}")
print(f"  - Cash & Short-term Investments: {format_billions(aapl_data.get('cashAndShortTermInvestments', aapl_data['cashAndCashEquivalents']))}")
print(f"  - Total Debt: {format_billions(aapl_data['totalDebt'])}")
print(f"  - Net Debt: {format_billions(aapl_data.get('netDebt', 0))}")

print("\nMICROSOFT (MSFT):")
print(f"  - Total Assets: {format_billions(msft_data['totalAssets'])}")
print(f"  - Total Equity: {format_billions(msft_data['totalStockholdersEquity'])}")
print(f"  - Cash & Short-term Investments: {format_billions(msft_data.get('cashAndShortTermInvestments', msft_data['cashAndCashEquivalents']))}")
print(f"  - Total Debt: {format_billions(msft_data['totalDebt'])}")
print(f"  - Net Debt: {format_billions(msft_data.get('netDebt', 0))}")

print("\n" + "-" * 80)
print("Analysis complete.")
