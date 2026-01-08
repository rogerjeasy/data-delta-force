"""
BQ4 Macro Nowcasting - Python Statistical Analysis (Fixed)
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from scipy.stats import pearsonr
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Load data
df = pd.read_csv('bq4_nowcasting_data.csv', parse_dates=['date'])

print("="*60)
print("BQ4: MACRO NOWCASTING ENHANCEMENT - STATISTICAL ANALYSIS")
print("="*60)

# ============================================================================
# DATA QUALITY CHECK
# ============================================================================
print("\n" + "="*60)
print("DATA QUALITY ASSESSMENT")
print("="*60)

print(f"\nDataset Coverage:")
print(f"  Date Range:          {df['date'].min()} to {df['date'].max()}")
print(f"  Total Records:       {len(df)}")
print(f"  Valid CPI:           {df['cpi'].notna().sum()} ({df['cpi'].notna().sum()/len(df)*100:.1f}%)")
print(f"  Valid Core CPI:      {df['core_cpi'].notna().sum()} ({df['core_cpi'].notna().sum()/len(df)*100:.1f}%)")
print(f"  Valid PCE:           {df['pce'].notna().sum()} ({df['pce'].notna().sum()/len(df)*100:.1f}%)")
print(f"  Valid BTC Vol:       {df['btc_volatility_30d'].notna().sum()} ({df['btc_volatility_30d'].notna().sum()/len(df)*100:.1f}%)")
print(f"  Valid Vol Spikes:    {df['btc_volatility_spike_flag'].sum()}")
print(f"  Regime Changes (7d): {df['regime_change_next_7d'].sum()}")

if df['cpi'].notna().sum() < 10:
    print(f"\n⚠️  WARNING: Only {df['cpi'].notna().sum()} CPI observations available!")
    print(f"   CPI data appears to be sparse in this dataset.")
    print(f"   Analysis will be limited to available data points.")

# ============================================================================
# H4a: Lead-lag correlation (using available data)
# ============================================================================
print("\n" + "="*60)
print("H4a: Bitcoin Volatility Leads CPI Surprises")
print("="*60)

# 15-day lead
valid_15d = df[['btc_vol_lag_15d', 'cpi_surprise_next_15d']].dropna()

if len(valid_15d) >= 10:
    # Check for constant arrays
    if valid_15d['cpi_surprise_next_15d'].std() == 0:
        print(f"\n15-Day Lead:")
        print(f"  Observations: {len(valid_15d)}")
        print(f"  ⚠️  CPI surprise is CONSTANT (no variation)")
        print(f"  Status:       NOT COMPUTABLE")
        print(f"  H4a Target:   ✗ NOT VALIDATED (insufficient variation)")
    else:
        corr_15d, pval_15d = pearsonr(
            valid_15d['btc_vol_lag_15d'], 
            valid_15d['cpi_surprise_next_15d']
        )
        r2_15d = corr_15d ** 2
        print(f"\n15-Day Lead:")
        print(f"  Observations: {len(valid_15d)}")
        print(f"  Correlation:  r = {corr_15d:.4f}")
        print(f"  P-value:      p = {pval_15d:.4f}")
        print(f"  R-squared:    R² = {r2_15d:.4f} ({r2_15d*100:.2f}%)")
        print(f"  Status:       {'SIGNIFICANT' if pval_15d < 0.05 else 'NOT SIGNIFICANT'} (p < 0.05)")
        print(f"  H4a Target:   {'✓ VALIDATED' if abs(corr_15d) > 0.15 else '✗ NOT VALIDATED'} (|r| > 0.15)")
else:
    print(f"\n15-Day Lead:")
    print(f"  Observations: {len(valid_15d)}")
    print(f"  ✗ INSUFFICIENT DATA (need ≥10 observations)")

# 30-day lead
valid_30d = df[['btc_vol_lag_30d', 'cpi_surprise_next_30d']].dropna()

if len(valid_30d) >= 10:
    if valid_30d['cpi_surprise_next_30d'].std() == 0:
        print(f"\n30-Day Lead:")
        print(f"  Observations: {len(valid_30d)}")
        print(f"  ⚠️  CPI surprise is CONSTANT (no variation)")
        print(f"  Status:       NOT COMPUTABLE")
        print(f"  H4a Target:   ✗ NOT VALIDATED (insufficient variation)")
    else:
        corr_30d, pval_30d = pearsonr(
            valid_30d['btc_vol_lag_30d'], 
            valid_30d['cpi_surprise_next_30d']
        )
        r2_30d = corr_30d ** 2
        print(f"\n30-Day Lead:")
        print(f"  Observations: {len(valid_30d)}")
        print(f"  Correlation:  r = {corr_30d:.4f}")
        print(f"  P-value:      p = {pval_30d:.4f}")
        print(f"  R-squared:    R² = {r2_30d:.4f} ({r2_30d*100:.2f}%)")
        print(f"  Status:       {'SIGNIFICANT' if pval_30d < 0.05 else 'NOT SIGNIFICANT'} (p < 0.05)")
        print(f"  H4a Target:   {'✓ VALIDATED' if abs(corr_30d) > 0.15 else '✗ NOT VALIDATED'} (|r| > 0.15)")
else:
    print(f"\n30-Day Lead:")
    print(f"  Observations: {len(valid_30d)}")
    print(f"  ✗ INSUFFICIENT DATA (need ≥10 observations)")

# ============================================================================
# H4b: RMSE Reduction (using available forecasts)
# ============================================================================
print("\n" + "="*60)
print("H4b: Crypto-Enhanced Forecast Accuracy")
print("="*60)

valid_forecasts = df[['cpi', 'baseline_cpi_forecast', 'crypto_enhanced_cpi_forecast']].dropna()

if len(valid_forecasts) >= 10:
    # Check if baseline/crypto forecasts are identical (no variation)
    baseline_errors = valid_forecasts['cpi'] - valid_forecasts['baseline_cpi_forecast']
    crypto_errors = valid_forecasts['cpi'] - valid_forecasts['crypto_enhanced_cpi_forecast']
    
    if baseline_errors.std() == 0:
        print(f"\n⚠️  Baseline forecast errors are CONSTANT")
        print(f"  This likely means CPI data has insufficient variation.")
        print(f"  RMSE calculation is not meaningful.")
        print(f"  H4b Target: ✗ NOT VALIDATED (insufficient data variation)")
    else:
        baseline_rmse = np.sqrt(mean_squared_error(
            valid_forecasts['cpi'], 
            valid_forecasts['baseline_cpi_forecast']
        ))
        baseline_mae = mean_absolute_error(
            valid_forecasts['cpi'], 
            valid_forecasts['baseline_cpi_forecast']
        )
        
        crypto_rmse = np.sqrt(mean_squared_error(
            valid_forecasts['cpi'], 
            valid_forecasts['crypto_enhanced_cpi_forecast']
        ))
        crypto_mae = mean_absolute_error(
            valid_forecasts['cpi'], 
            valid_forecasts['crypto_enhanced_cpi_forecast']
        )
        
        # Safe division
        rmse_reduction_pct = ((baseline_rmse - crypto_rmse) / baseline_rmse * 100) if baseline_rmse > 0 else 0
        mae_reduction_pct = ((baseline_mae - crypto_mae) / baseline_mae * 100) if baseline_mae > 0 else 0
        
        print(f"\nBaseline Model (Naive Forecast):")
        print(f"  RMSE: {baseline_rmse:.4f}")
        print(f"  MAE:  {baseline_mae:.4f}")
        print(f"  N:    {len(valid_forecasts)}")
        
        print(f"\nCrypto-Enhanced Model:")
        print(f"  RMSE: {crypto_rmse:.4f}")
        print(f"  MAE:  {crypto_mae:.4f}")
        print(f"  N:    {len(valid_forecasts)}")
        
        print(f"\nImprovement:")
        print(f"  RMSE Reduction: {rmse_reduction_pct:+.2f}%")
        print(f"  MAE Reduction:  {mae_reduction_pct:+.2f}%")
        print(f"  H4b Target:     {'✓ VALIDATED' if rmse_reduction_pct > 15 else '✗ NOT VALIDATED'} (>15% reduction)")
else:
    print(f"\nObservations: {len(valid_forecasts)}")
    print(f"✗ INSUFFICIENT DATA (need ≥10 forecast pairs)")
    print(f"  Available CPI: {df['cpi'].notna().sum()}")
    print(f"  Available Forecasts: {len(valid_forecasts)}")
    print(f"  H4b Target: ✗ NOT VALIDATED (insufficient data)")

# ============================================================================
# Incremental R² (if possible)
# ============================================================================
print("\n" + "="*60)
print("Incremental R² from Crypto Variables")
print("="*60)

if len(valid_forecasts) >= 10 and baseline_errors.std() > 0:
    baseline_r2 = r2_score(
        valid_forecasts['cpi'], 
        valid_forecasts['baseline_cpi_forecast']
    )
    crypto_r2 = r2_score(
        valid_forecasts['cpi'], 
        valid_forecasts['crypto_enhanced_cpi_forecast']
    )
    incremental_r2 = crypto_r2 - baseline_r2
    
    print(f"\nBaseline Model R²:       {baseline_r2:.4f} ({baseline_r2*100:.2f}%)")
    print(f"Crypto-Enhanced R²:      {crypto_r2:.4f} ({crypto_r2*100:.2f}%)")
    print(f"Incremental R²:          {incremental_r2:+.4f} ({incremental_r2*100:+.2f}%)")
    print(f"Target (R² > 0.10):      {'✓ VALIDATED' if incremental_r2 > 0.10 else '✗ NOT VALIDATED'}")
else:
    print(f"\n✗ INSUFFICIENT DATA for R² calculation")

# ============================================================================
# H4c: Regime Prediction (Modified)
# ============================================================================
print("\n" + "="*60)
print("H4c: Regime Transition Prediction (BTC Volatility)")
print("="*60)

valid_regime = df[['btc_volatility_spike_flag', 'regime_change_next_7d']].dropna()

if len(valid_regime) >= 20:
    X = valid_regime[['btc_volatility_spike_flag']].astype(int)
    y = valid_regime['regime_change_next_7d'].astype(int)
    
    total_regime_changes = y.sum()
    total_vol_spikes = X['btc_volatility_spike_flag'].sum()
    detected_by_vol_spike = ((X['btc_volatility_spike_flag'] == 1) & (y == 1)).sum()
    
    detection_rate = (detected_by_vol_spike / total_regime_changes * 100) if total_regime_changes > 0 else 0
    
    # Logistic regression (if variation exists)
    if total_vol_spikes > 0 and total_regime_changes > 0:
        model = LogisticRegression(max_iter=1000)
        model.fit(X, y)
        accuracy = model.score(X, y)
        
        print(f"\nUsing BTC Volatility Spikes as Predictor:")
        print(f"  Total Observations:      {len(valid_regime)}")
        print(f"  Regime Changes (7d):     {total_regime_changes}")
        print(f"  Volatility Spikes:       {total_vol_spikes}")
        print(f"  Detected by Vol Spike:   {detected_by_vol_spike}")
        print(f"  Detection Rate:          {detection_rate:.2f}%")
        print(f"  Model Accuracy:          {accuracy*100:.2f}%")
        print(f"  H4c Modified Target:     {'✓ VALIDATED' if detection_rate > 50 else '✗ NOT VALIDATED'} (>50% detection)")
    else:
        print(f"\nInsufficient variation for logistic regression:")
        print(f"  Regime Changes: {total_regime_changes}")
        print(f"  Volatility Spikes: {total_vol_spikes}")
        print(f"  H4c Target: ✗ NOT VALIDATED")
    
    print(f"\nNote: Original H4c required BTC-Gold correlation (unavailable).")
    print(f"      Modified to use BTC volatility spikes instead.")
else:
    print(f"\n✗ INSUFFICIENT DATA (need ≥20 observations)")
    print(f"  Available: {len(valid_regime)}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "="*60)
print("HYPOTHESIS VALIDATION SUMMARY")
print("="*60)

print(f"\n{'Hypothesis':<20} {'Result':<20} {'Status':<15}")
print(f"{'-'*60}")

# H4a
if len(valid_30d) >= 10 and valid_30d['cpi_surprise_next_30d'].std() > 0:
    print(f"{'H4a (30-day)':<20} {'r=' + str(round(corr_30d, 4)):<20} {'NOT VALIDATED':<15}")
else:
    print(f"{'H4a (30-day)':<20} {'Insufficient data':<20} {'NOT TESTABLE':<15}")

# H4b
if len(valid_forecasts) >= 10 and baseline_errors.std() > 0:
    print(f"{'H4b (RMSE)':<20} {str(round(rmse_reduction_pct, 2)) + '%':<20} {'VALIDATED' if rmse_reduction_pct > 15 else 'NOT VALIDATED':<15}")
else:
    print(f"{'H4b (RMSE)':<20} {'Insufficient data':<20} {'NOT TESTABLE':<15}")

# H4c
if len(valid_regime) >= 20 and total_vol_spikes > 0:
    print(f"{'H4c (Modified)':<20} {str(round(detection_rate, 2)) + '%':<20} {'VALIDATED' if detection_rate > 50 else 'NOT VALIDATED':<15}")
else:
    print(f"{'H4c (Modified)':<20} {'Insufficient data':<20} {'NOT TESTABLE':<15}")

print("\n" + "="*60)
print("ANALYSIS COMPLETE")
print("="*60)
print(f"\n⚠️  NOTE: CPI data coverage is {df['cpi'].notna().sum()}/{len(df)} observations")
print(f"   Limited macro data restricts nowcasting model validation.")
print(f"   Consider extending dataset to include more CPI release dates.")