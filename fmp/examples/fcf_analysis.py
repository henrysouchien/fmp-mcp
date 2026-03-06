"""
Free Cash Flow Analysis for GOOGL, META, and AMZN

Analyzes:
1. Free cash flow trend for each company
2. FCF margin (FCF / Revenue)
3. Capital expenditure intensity
4. Which company generates the most FCF per dollar of revenue
"""


import pandas as pd
from fmp import FMPClient

# Initialize client
fmp = FMPClient()

# Companies to analyze
tickers = ['GOOGL', 'META', 'AMZN']

# Fetch cash flow and income statement data for each company
print("Fetching data for GOOGL, META, and AMZN (last 3 years, annual)...")
print("=" * 80)

cash_flow_data = {}
income_data = {}

for ticker in tickers:
    # Fetch cash flow statement (last 3 years)
    cf = fmp.fetch('cash_flow', symbol=ticker, period='annual', limit=3)
    cash_flow_data[ticker] = cf

    # Fetch income statement for revenue (last 3 years)
    inc = fmp.fetch('income_statement', symbol=ticker, period='annual', limit=3)
    income_data[ticker] = inc

# Display available columns in cash flow data
print("\nCash Flow Statement Columns:")
print(cash_flow_data['GOOGL'].columns.tolist())

print("\nIncome Statement Columns:")
print(income_data['GOOGL'].columns.tolist())

# Analysis 1: Free Cash Flow Trend
print("\n" + "=" * 80)
print("1. FREE CASH FLOW TREND (Last 3 Years)")
print("=" * 80)

fcf_summary = []

for ticker in tickers:
    cf = cash_flow_data[ticker].copy()
    cf = cf.sort_values('date', ascending=True)  # Sort by date ascending

    print(f"\n{ticker}:")

    # Free Cash Flow is typically Operating Cash Flow - CapEx
    # Check column names
    if 'freeCashFlow' in cf.columns:
        fcf_col = 'freeCashFlow'
    elif 'free_cash_flow' in cf.columns:
        fcf_col = 'free_cash_flow'
    else:
        # Calculate FCF = Operating Cash Flow - Capital Expenditures
        if 'operatingCashFlow' in cf.columns:
            ocf_col = 'operatingCashFlow'
        elif 'netCashProvidedByOperatingActivities' in cf.columns:
            ocf_col = 'netCashProvidedByOperatingActivities'
        else:
            print(f"  Cannot find operating cash flow column")
            continue

        if 'capitalExpenditure' in cf.columns:
            capex_col = 'capitalExpenditure'
        elif 'capitalExpenditures' in cf.columns:
            capex_col = 'capitalExpenditures'
        else:
            print(f"  Cannot find capital expenditure column")
            continue

        cf['freeCashFlow'] = cf[ocf_col] + cf[capex_col]  # CapEx is usually negative
        fcf_col = 'freeCashFlow'

    for _, row in cf.iterrows():
        year = row['date'][:4] if 'date' in row else row.get('calendarYear', 'N/A')
        fcf = row[fcf_col]
        fcf_billions = fcf / 1e9
        print(f"  {year}: ${fcf_billions:,.2f}B")

        fcf_summary.append({
            'Ticker': ticker,
            'Year': year,
            'FCF ($B)': fcf_billions
        })

# Create FCF summary dataframe
fcf_df = pd.DataFrame(fcf_summary)
fcf_pivot = fcf_df.pivot(index='Year', columns='Ticker', values='FCF ($B)')
print("\nFree Cash Flow Summary ($B):")
print(fcf_pivot.to_string())

# Analysis 2: FCF Margin
print("\n" + "=" * 80)
print("2. FCF MARGIN (FCF / Revenue)")
print("=" * 80)

margin_summary = []

for ticker in tickers:
    cf = cash_flow_data[ticker].copy()
    inc = income_data[ticker].copy()

    cf = cf.sort_values('date', ascending=True)
    inc = inc.sort_values('date', ascending=True)

    print(f"\n{ticker}:")

    # Get FCF
    if 'freeCashFlow' in cf.columns:
        fcf_col = 'freeCashFlow'
    else:
        if 'operatingCashFlow' in cf.columns:
            ocf_col = 'operatingCashFlow'
        elif 'netCashProvidedByOperatingActivities' in cf.columns:
            ocf_col = 'netCashProvidedByOperatingActivities'
        cf['freeCashFlow'] = cf[ocf_col] + cf.get('capitalExpenditure', cf.get('capitalExpenditures', 0))
        fcf_col = 'freeCashFlow'

    # Get revenue
    if 'revenue' in inc.columns:
        rev_col = 'revenue'
    elif 'totalRevenue' in inc.columns:
        rev_col = 'totalRevenue'
    else:
        print(f"  Cannot find revenue column")
        continue

    # Match by year
    cf['year'] = cf['date'].str[:4]
    inc['year'] = inc['date'].str[:4]

    for _, cf_row in cf.iterrows():
        year = cf_row['year']
        inc_row = inc[inc['year'] == year]

        if not inc_row.empty:
            fcf = cf_row[fcf_col]
            revenue = inc_row.iloc[0][rev_col]
            margin = (fcf / revenue) * 100

            print(f"  {year}: FCF ${fcf/1e9:,.2f}B / Revenue ${revenue/1e9:,.2f}B = {margin:.1f}%")

            margin_summary.append({
                'Ticker': ticker,
                'Year': year,
                'FCF Margin (%)': round(margin, 1)
            })

margin_df = pd.DataFrame(margin_summary)
margin_pivot = margin_df.pivot(index='Year', columns='Ticker', values='FCF Margin (%)')
print("\nFCF Margin Summary (%):")
print(margin_pivot.to_string())

# Analysis 3: Capital Expenditure Intensity
print("\n" + "=" * 80)
print("3. CAPITAL EXPENDITURE INTENSITY (CapEx / Revenue)")
print("=" * 80)

capex_summary = []

for ticker in tickers:
    cf = cash_flow_data[ticker].copy()
    inc = income_data[ticker].copy()

    cf = cf.sort_values('date', ascending=True)
    inc = inc.sort_values('date', ascending=True)

    print(f"\n{ticker}:")

    # Get CapEx (usually negative)
    if 'capitalExpenditure' in cf.columns:
        capex_col = 'capitalExpenditure'
    elif 'capitalExpenditures' in cf.columns:
        capex_col = 'capitalExpenditures'
    else:
        print(f"  Cannot find capital expenditure column")
        continue

    # Get revenue
    if 'revenue' in inc.columns:
        rev_col = 'revenue'
    elif 'totalRevenue' in inc.columns:
        rev_col = 'totalRevenue'

    cf['year'] = cf['date'].str[:4]
    inc['year'] = inc['date'].str[:4]

    for _, cf_row in cf.iterrows():
        year = cf_row['year']
        inc_row = inc[inc['year'] == year]

        if not inc_row.empty:
            capex = abs(cf_row[capex_col])  # Use absolute value
            revenue = inc_row.iloc[0][rev_col]
            intensity = (capex / revenue) * 100

            print(f"  {year}: CapEx ${capex/1e9:,.2f}B / Revenue ${revenue/1e9:,.2f}B = {intensity:.1f}%")

            capex_summary.append({
                'Ticker': ticker,
                'Year': year,
                'CapEx Intensity (%)': round(intensity, 1)
            })

capex_df = pd.DataFrame(capex_summary)
capex_pivot = capex_df.pivot(index='Year', columns='Ticker', values='CapEx Intensity (%)')
print("\nCapEx Intensity Summary (%):")
print(capex_pivot.to_string())

# Analysis 4: FCF Efficiency Ranking
print("\n" + "=" * 80)
print("4. FCF EFFICIENCY RANKING (Average FCF Margin)")
print("=" * 80)

avg_margins = margin_df.groupby('Ticker')['FCF Margin (%)'].mean().sort_values(ascending=False)
print("\nAverage FCF Margin (3-Year):")
for ticker, margin in avg_margins.items():
    print(f"  {ticker}: {margin:.1f}%")

winner = avg_margins.idxmax()
print(f"\n*** {winner} generates the most FCF per dollar of revenue! ***")

# Summary Table
print("\n" + "=" * 80)
print("COMPREHENSIVE SUMMARY")
print("=" * 80)

summary_data = []
for ticker in tickers:
    cf = cash_flow_data[ticker].copy()
    inc = income_data[ticker].copy()

    # Most recent year
    cf = cf.sort_values('date', ascending=False)
    inc = inc.sort_values('date', ascending=False)

    latest_cf = cf.iloc[0]
    latest_inc = inc.iloc[0]

    # FCF
    if 'freeCashFlow' in cf.columns:
        fcf = latest_cf['freeCashFlow']
    else:
        ocf_col = 'operatingCashFlow' if 'operatingCashFlow' in cf.columns else 'netCashProvidedByOperatingActivities'
        capex_col = 'capitalExpenditure' if 'capitalExpenditure' in cf.columns else 'capitalExpenditures'
        fcf = latest_cf[ocf_col] + latest_cf[capex_col]

    # Revenue
    rev_col = 'revenue' if 'revenue' in inc.columns else 'totalRevenue'
    revenue = latest_inc[rev_col]

    # CapEx
    capex_col = 'capitalExpenditure' if 'capitalExpenditure' in cf.columns else 'capitalExpenditures'
    capex = abs(latest_cf[capex_col])

    # OCF
    ocf_col = 'operatingCashFlow' if 'operatingCashFlow' in cf.columns else 'netCashProvidedByOperatingActivities'
    ocf = latest_cf[ocf_col]

    summary_data.append({
        'Company': ticker,
        'Latest Year': latest_cf['date'][:4],
        'Revenue ($B)': round(revenue / 1e9, 1),
        'Operating CF ($B)': round(ocf / 1e9, 1),
        'CapEx ($B)': round(capex / 1e9, 1),
        'FCF ($B)': round(fcf / 1e9, 1),
        'FCF Margin (%)': round((fcf / revenue) * 100, 1),
        'CapEx Intensity (%)': round((capex / revenue) * 100, 1)
    })

summary_df = pd.DataFrame(summary_data)
print("\nLatest Year Comparison:")
print(summary_df.to_string(index=False))

print("\n" + "=" * 80)
print("KEY INSIGHTS")
print("=" * 80)
print(f"""
1. FCF Generation: {winner} leads with the highest average FCF margin,
   indicating superior efficiency in converting revenue to free cash flow.

2. CapEx Intensity: Higher CapEx intensity (as % of revenue) indicates more
   capital-intensive operations, which can constrain FCF in the near term
   but may support future growth.

3. Investment Consideration: Companies with high FCF margins have more
   flexibility for shareholder returns (dividends, buybacks) and strategic
   investments without requiring external financing.
""")
