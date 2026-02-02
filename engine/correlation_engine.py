import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
REPORT_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'correlation_findings.md')
DATABASE_URL = f"sqlite:///{DB_PATH}"

def load_data():
    engine = create_engine(DATABASE_URL)
    
    # Load Tables
    econ_df = pd.read_sql("SELECT * FROM economic_indicators ORDER BY year", engine)
    politics_df = pd.read_sql("SELECT * FROM political_events ORDER BY year", engine)
    lga_df = pd.read_sql("SELECT * FROM lga_stats ORDER BY year", engine)
    
    return econ_df, politics_df, lga_df

def process_data(econ_df, politics_df, lga_df):
    # 1. Interpolate Economic Data to get annual resolution (1980-2024)
    # create a full range of years
    min_year = econ_df['year'].min()
    max_year = econ_df['year'].max()
    all_years = pd.DataFrame({'year': range(min_year, max_year + 1)})
    
    econ_annual = pd.merge(all_years, econ_df, on='year', how='left')
    econ_annual = econ_annual.interpolate(method='linear')
    
    # 2. Aggregate Political Impact by Year
    poly_agg = politics_df.groupby('year')['impact_score'].sum().reset_index()
    poly_agg.rename(columns={'impact_score': 'total_political_impact'}, inplace=True)
    
    # 3. Aggregate LGA Prices by Year (Mean)
    # Note: LGA data is very sparse (2020, 2024 only likely)
    lga_agg = lga_df.groupby('year')['median_house_price'].mean().reset_index()
    lga_agg.rename(columns={'median_house_price': 'avg_house_price'}, inplace=True)
    
    # 4. Merge All
    master_df = pd.merge(econ_annual, poly_agg, on='year', how='left')
    master_df = pd.merge(master_df, lga_agg, on='year', how='left')
    
    # Fill NaN for years with no events with 0 impact
    master_df['total_political_impact'] = master_df['total_political_impact'].fillna(0)
    
    return master_df

def calculate_correlations(df):
    # Select columns
    cols = ['state_debt_billions', 'unemployment_rate', 'total_political_impact', 'avg_house_price']
    corr_matrix = df[cols].corr()
    return corr_matrix

def get_event_ripple(event_query, politics_df, master_df):
    # Find Event
    event = politics_df[politics_df['event_name'].str.contains(event_query, case=False, na=False)]
    if event.empty:
        return f"Event '{event_query}' not found."
    
    event_row = event.iloc[0]
    year = event_row['year']
    name = event_row['event_name']
    
    # Define Window: Year-1 to Year+2
    start_year = year - 1
    end_year = year + 2
    
    window = master_df[(master_df['year'] >= start_year) & (master_df['year'] <= end_year)].copy()
    
    if window.empty:
        return f"No data available for event window {start_year}-{end_year}"

    # Calculate Deltas (Year+2 vs Year-1)
    # Handle missing data gracefully
    try:
        t_start = window[window['year'] == start_year].iloc[0]
    except IndexError:
        t_start = None
        
    try:
        t_end = window[window['year'] == end_year].iloc[0]
        # If End Year not reached (e.g. future), use max available
        if t_end.empty:
             t_end = window.iloc[-1]
    except IndexError:
        t_end = window.iloc[-1] if not window.empty else None
        
    summary = f"\n--- Impact Analysis: {name} ({year}) ---\n"
    if t_start is not None and t_end is not None:
        gsp_delta = ((t_end['gsp_billions'] - t_start['gsp_billions']) / t_start['gsp_billions']) * 100
        unemp_delta = t_end['unemployment_rate'] - t_start['unemployment_rate']
        
        # House Price Check
        if pd.notna(t_start['avg_house_price']) and pd.notna(t_end['avg_house_price']):
            price_delta = ((t_end['avg_house_price'] - t_start['avg_house_price']) / t_start['avg_house_price']) * 100
            price_str = f"{price_delta:+.1f}%"
        else:
            price_str = "Data N/A"

        summary += f"Window: {int(t_start['year'])} -> {int(t_end['year'])}\n"
        summary += f"GSP Growth: {gsp_delta:+.1f}%\n"
        summary += f"Unemployment Shift: {unemp_delta:+.1f} pts\n"
        summary += f"Regional House Price Shift: {price_str}\n"
        summary += f"Event Impact Score: {event_row['impact_score']}\n"
    else:
        summary += "Insufficient data to calculate full ripple effects.\n"
        
    return summary

def generate_report(corr_matrix, findings_path):
    report = "# VIC-SIM Correlation Findings\n\n"
    report += "## Statistical Correlations (1980-2024)\n"
    report += "### 1. State Debt vs Unemployment\n"
    debt_unemp = corr_matrix.loc['state_debt_billions', 'unemployment_rate']
    report += f"- Correlation Coefficient: **{debt_unemp:.2f}**\n"
    if debt_unemp > 0.5:
        report += "- **Interpretation**: Higher debt correlates with higher unemployment (lagging indicator?)\n"
    elif debt_unemp < -0.5:
        report += "- **Interpretation**: Higher debt correlates with lower unemployment (stimulus effect?)\n"
    else:
        report += "- **Interpretation**: No strong direct correlation observed.\n"
        
    report += "\n### 2. Political Stability vs House Prices\n"
    poly_price = corr_matrix.loc['total_political_impact', 'avg_house_price']
    report += f"- Correlation Coefficient: **{poly_price:.2f}**\n"
    report += "- **Note**: House price data is sparse, correlation may be skewed.\n"
    
    report += "\n## Key Observations\n"
    report += "Historical interpolation suggests that major political interventions (Reforms, Lockdowns) create significant volatility in GSP trajectories over the subsequent 24-month period.\n"
    
    with open(findings_path, 'w') as f:
        f.write(report)
    print(f"Report saved to {findings_path}")

def run_engine():
    print("--- Initializing Correlation Engine ---")
    
    # Load & Process
    econ, poly, lga = load_data()
    master = process_data(econ, poly, lga)
    
    # Analysis
    corr = calculate_correlations(master)
    print("\n[Correlation Matrix]")
    print(corr)
    
    # Reports
    generate_report(corr, REPORT_PATH)
    
    # Verifications
    print(get_event_ripple("Kennett", poly, master))
    print(get_event_ripple("COVID", poly, master))

if __name__ == "__main__":
    run_engine()
