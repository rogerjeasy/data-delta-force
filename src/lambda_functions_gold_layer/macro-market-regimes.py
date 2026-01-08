import json
import boto3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

BUCKET_NAME = 'crypto-macro-datalake-ddf-v2'
BRONZE_PREFIX = 'bronze/static/market_regimes/'


def lambda_handler(event, context):
    print("📊 Starting Market Regime Classification...")
    
    s3 = boto3.client('s3')
    
    try:
        # Download market data
        data = download_market_data()
        
        # Calculate regimes
        data = classify_bull_bear(data)
        data = classify_vix_regime(data)
        data = create_combined_regime(data)
        
        # Save to S3
        s3_key = generate_s3_key()
        save_to_s3(s3, data, s3_key)
        
        print(f"✅ Success! Processed {len(data)} days")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'days_processed': len(data),
                's3_location': f's3://{BUCKET_NAME}/{s3_key}'
            })
        }
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}


def download_market_data(lookback_days=365):
    """Download S&P 500 and VIX data"""
    print(f"📥 Downloading last {lookback_days} days of market data...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    # Alternative Methode: Ticker().history() statt download()
    try:
        sp500_ticker = yf.Ticker('^GSPC')
        sp500_data = sp500_ticker.history(start=start_date, end=end_date)
        
        vix_ticker = yf.Ticker('^VIX')
        vix_data = vix_ticker.history(start=start_date, end=end_date)
        
        # Combine
        data = pd.DataFrame({
            'date': sp500_data.index,
            'sp500_close': sp500_data['Close'].values,
            'vix_close': vix_data['Close'].values
        })
        
        data = data.dropna()
        print(f"✅ Downloaded {len(data)} days")
        return data
        
    except Exception as e:
        print(f"❌ Download failed: {e}")
        raise

def calculate_drawdown(prices):
    """Calculate drawdown from peak"""
    cummax = prices.cummax()
    drawdown = (prices - cummax) / cummax * 100
    return drawdown


def classify_bull_bear(data):
    """Classify Bull/Bear markets (20% threshold)"""
    print("📊 Classifying Bull/Bear markets...")
    
    data['drawdown'] = calculate_drawdown(data['sp500_close'])
    data['bull_bear_regime'] = data['drawdown'].apply(
        lambda x: 'Bear' if x <= -20 else 'Bull'
    )
    
    return data


def classify_vix_regime(data):
    """Classify volatility regimes"""
    print("📊 Classifying VIX regimes...")
    
    def vix_category(vix):
        if vix < 20:
            return 'Low_Volatility'
        elif vix < 30:
            return 'Medium_Volatility'
        else:
            return 'High_Volatility'
    
    data['vix_regime'] = data['vix_close'].apply(vix_category)
    return data


def create_combined_regime(data):
    """Create combined market regime"""
    print("📊 Creating combined regime...")
    
    def combined_regime(row):
        if row['bull_bear_regime'] == 'Bull' and row['vix_regime'] == 'Low_Volatility':
            return 'Bull_Low_Vol'
        elif row['bull_bear_regime'] == 'Bull':
            return 'Bull_High_Vol'
        elif row['vix_regime'] == 'High_Volatility':
            return 'Bear_High_Vol'
        else:
            return 'Bear_Medium_Vol'
    
    data['market_regime'] = data.apply(combined_regime, axis=1)
    return data


def generate_s3_key():
    """Generate S3 key"""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    return f"{BRONZE_PREFIX}market_regimes_{timestamp}.csv"


def save_to_s3(s3_client, data, s3_key):
    """Save to S3 as CSV"""
    print(f"⬆️ Saving to s3://{BUCKET_NAME}/{s3_key}")
    
    csv_buffer = data.to_csv(index=False)
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.encode('utf-8'),
        ContentType='text/csv'
    )
    
    print("✅ Saved to S3!")