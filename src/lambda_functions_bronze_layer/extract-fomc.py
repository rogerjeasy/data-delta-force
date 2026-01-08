"""
AWS Lambda Function: Extract FOMC Sentiment Data (Extended - 100 Minutes)
Processes FOMC meeting minutes in batches to handle large volumes
Uses DynamoDB for state tracking to resume across invocations
"""

import json
import boto3
import urllib3
from datetime import datetime
from typing import Dict, Any, List, Optional
import os
import re
import time
from decimal import Decimal

# Initialize clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
http = urllib3.PoolManager()

# Configuration
S3_BUCKET = os.environ['S3_BUCKET']
STATE_TABLE_NAME = os.environ.get('STATE_TABLE_NAME', 'fomc-extraction-state')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))  # Process 20 per invocation
MAX_MINUTES = int(os.environ.get('MAX_MINUTES', '100'))  # Total to process

# FOMC URLs
FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

# Sentiment lexicon
POSITIVE_WORDS = {
    'growth', 'strong', 'increase', 'improve', 'positive', 'robust', 'expansion',
    'higher', 'accelerat', 'solid', 'favorable', 'gain', 'progress', 'strength',
    'accommodative', 'support', 'recovery', 'confident', 'optimistic', 'upgrade',
    'momentum', 'resilient', 'stable', 'sustained', 'encouraging'
}

NEGATIVE_WORDS = {
    'concern', 'risk', 'uncertain', 'decline', 'weak', 'slow', 'lower', 'pressure',
    'challenge', 'difficult', 'tension', 'volatility', 'downside', 'modest', 'elevated',
    'constraint', 'headwind', 'deteriorat', 'soften', 'caution', 'slowing', 'weaken',
    'subdued', 'fragile', 'turbulence', 'stress', 'tighten', 'restrictive'
}


def lambda_handler(event, context):
    """AWS Lambda handler with batch processing support"""
    print("=" * 80)
    print("🏛️ FOMC Sentiment Extraction - Extended (Batch Processing)")
    print("=" * 80)
    
    timestamp = datetime.utcnow()
    execution_id = event.get('execution_id', timestamp.strftime('%Y%m%d_%H%M%S'))
    
    print(f"⏰ Execution time: {timestamp.isoformat()}")
    print(f"🔖 Execution ID: {execution_id}")
    print(f"📦 Batch size: {BATCH_SIZE} minutes per invocation")
    print(f"🎯 Target total: {MAX_MINUTES} minutes")
    
    try:
        # Get or initialize state
        state = get_execution_state(execution_id)
        
        if state['status'] == 'completed':
            print("✅ Extraction already completed for this execution")
            return create_response(200, state, execution_id)
        
        # Scrape FOMC links if not cached
        if not state['all_links']:
            print("\n🔍 Scraping FOMC meeting minutes links...")
            all_links = scrape_fomc_links()
            print(f"📄 Found {len(all_links)} total FOMC minutes links")
            
            # Limit to MAX_MINUTES
            all_links = all_links[:MAX_MINUTES]
            state['all_links'] = all_links
            state['total_links'] = len(all_links)
        
        # Determine batch to process
        start_idx = state['processed_count']
        end_idx = min(start_idx + BATCH_SIZE, state['total_links'])
        batch_links = state['all_links'][start_idx:end_idx]
        
        print(f"\n📊 Processing batch: {start_idx + 1}-{end_idx} of {state['total_links']}")
        print("=" * 80)
        
        # Process batch
        batch_data = []
        for idx, link in enumerate(batch_links, start=start_idx + 1):
            print(f"\n[{idx}/{state['total_links']}] {link}")
            try:
                sentiment_data = analyze_fomc_minutes(link)
                batch_data.append(sentiment_data)
                print(f"  ✅ {sentiment_data['date']} - {sentiment_data['regime']}")
            except Exception as e:
                print(f"  ⚠️ Error: {e}")
                state['failed_links'].append({'url': link, 'error': str(e)})
                continue
            
            # Small delay to be polite to Fed servers
            time.sleep(0.5)
        
        # Save batch to S3
        if batch_data:
            batch_s3_key = save_batch_to_s3(batch_data, execution_id, start_idx, timestamp)
            state['batch_files'].append(batch_s3_key)
            print(f"\n✅ Batch saved: {batch_s3_key}")
        
        # Update state
        state['processed_count'] = end_idx
        state['successful_count'] += len(batch_data)
        state['last_updated'] = timestamp.isoformat()
        
        # Check if complete
        if state['processed_count'] >= state['total_links']:
            state['status'] = 'completed'
            state['completed_at'] = timestamp.isoformat()
            
            # Consolidate all batches into final file
            final_s3_key = consolidate_batches(state, execution_id, timestamp)
            state['final_file'] = final_s3_key
            
            print("\n" + "=" * 80)
            print("🎉 ALL BATCHES COMPLETE!")
            print(f"✅ Total processed: {state['successful_count']}/{state['total_links']}")
            print(f"❌ Failed: {len(state['failed_links'])}")
            print(f"📁 Final file: {final_s3_key}")
            print("=" * 80)
        else:
            state['status'] = 'in_progress'
            print("\n" + "=" * 80)
            print(f"⏸️ Batch complete. Progress: {state['processed_count']}/{state['total_links']}")
            print("🔄 Invoke Lambda again to continue processing")
            print("=" * 80)
        
        # Save state
        save_execution_state(execution_id, state)
        
        return create_response(200, state, execution_id)
        
    except Exception as e:
        print(f"❌ Lambda execution failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'FOMC extraction failed',
                'error': str(e),
                'execution_id': execution_id
            })
        }

def convert_decimals(obj):
    """Convert DynamoDB Decimal types to int/float"""
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj
        
def get_execution_state(execution_id: str) -> Dict:
    """Retrieve execution state from DynamoDB or initialize new state"""
    try:
        table = dynamodb.Table(STATE_TABLE_NAME)
        response = table.get_item(Key={'execution_id': execution_id})
        
        if 'Item' in response:
            print(f"📂 Resuming existing execution: {execution_id}")
            state = response['Item']
            
            # ✅ FIX: Convert Decimal to int
            state['total_links'] = int(state.get('total_links', 0))
            state['processed_count'] = int(state.get('processed_count', 0))
            state['successful_count'] = int(state.get('successful_count', 0))
            
            # Convert failed_links list if it contains Decimals
            if 'failed_links' in state and not isinstance(state['failed_links'], list):
                state['failed_links'] = []
            
            return state
    except Exception as e:
        print(f"⚠️ DynamoDB get error (will create new): {e}")
    
    # Initialize new state
    print(f"🆕 Initializing new execution: {execution_id}")
    return {
        'execution_id': execution_id,
        'status': 'in_progress',
        'all_links': [],
        'total_links': 0,
        'processed_count': 0,
        'successful_count': 0,
        'failed_links': [],
        'batch_files': [],
        'final_file': None,
        'started_at': datetime.utcnow().isoformat(),
        'last_updated': datetime.utcnow().isoformat(),
        'completed_at': None
    }

def save_execution_state(execution_id: str, state: Dict) -> None:
    """Save execution state to DynamoDB"""
    try:
        table = dynamodb.Table(STATE_TABLE_NAME)
        
        # Remove None values and ensure proper types
        clean_state = {k: v for k, v in state.items() if v is not None}
        
        # Ensure numeric fields are proper types
        if 'total_links' in clean_state:
            clean_state['total_links'] = int(clean_state['total_links'])
        if 'processed_count' in clean_state:
            clean_state['processed_count'] = int(clean_state['processed_count'])
        if 'successful_count' in clean_state:
            clean_state['successful_count'] = int(clean_state['successful_count'])
        
        table.put_item(Item=clean_state)
        print(f"💾 State saved to DynamoDB")
    except Exception as e:
        print(f"⚠️ DynamoDB save error: {e}")


def scrape_fomc_links() -> List[str]:
    """Scrape FOMC meeting minutes links from Fed website"""
    try:
        response = http.request('GET', FOMC_CALENDAR_URL, timeout=30.0)
        
        if response.status != 200:
            raise Exception(f"Failed to fetch FOMC calendar: {response.status}")
        
        html_content = response.data.decode('utf-8')
        
        # Extract links from multiple patterns
        links = []
        
        # Pattern 1: /monetarypolicy/fomcminutes20231213.htm
        pattern1 = re.findall(r'href="(/monetarypolicy/fomcminutes\d+\.htm)"', html_content)
        
        # Pattern 2: Full URLs
        pattern2 = re.findall(r'href="(https://www\.federalreserve\.gov/monetarypolicy/fomcminutes\d+\.htm)"', html_content)
        
        # Combine and normalize
        for link in pattern1:
            full_link = f"https://www.federalreserve.gov{link}"
            if full_link not in links:
                links.append(full_link)
        
        for link in pattern2:
            if link not in links:
                links.append(link)
        
        # Sort by date (newest first) - extract date from URL
        def extract_date_from_url(url):
            match = re.search(r'fomcminutes(\d{8})\.htm', url)
            return match.group(1) if match else '00000000'
        
        links.sort(key=extract_date_from_url, reverse=True)
        
        return links
        
    except Exception as e:
        print(f"❌ Error scraping FOMC links: {e}")
        raise


def analyze_fomc_minutes(url: str) -> Dict[str, Any]:
    """Fetch FOMC minutes and perform sentiment analysis"""
    try:
        # Fetch the page
        response = http.request('GET', url, timeout=30.0)
        
        if response.status != 200:
            raise Exception(f"HTTP {response.status}")
        
        html_content = response.data.decode('utf-8')
        
        # Extract title
        title_match = re.search(r'<title>(.*?)</title>', html_content, re.IGNORECASE)
        title = title_match.group(1) if title_match else "FOMC Minutes"
        
        # Extract text content (remove HTML tags)
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Extract meeting date from title or URL
        date_str = extract_date_from_title(title)
        if not date_str:
            date_str = extract_date_from_url_fallback(url)
        
        # Perform sentiment analysis
        sentiment_scores = calculate_sentiment(text)
        
        # Word count
        word_count = len(text.split())
        
        # Determine regime
        compound = sentiment_scores['compound']
        regime = 'dovish' if compound > 0.05 else ('hawkish' if compound < -0.05 else 'neutral')
        
        return {
            'url': url,
            'title': title,
            'date': date_str,
            'year': date_str.split()[-1] if date_str and ' ' in date_str else None,
            'word_count': word_count,
            'sentiment_source': 'VADER_LITE',
            'text_excerpt': text[:500],
            'retrieved_on': datetime.utcnow().isoformat(),
            'compound': sentiment_scores['compound'],
            'positive': sentiment_scores['pos'],
            'negative': sentiment_scores['neg'],
            'neutral': sentiment_scores['neu'],
            'regime': regime
        }
        
    except Exception as e:
        raise Exception(f"Analysis failed: {str(e)}")


def extract_date_from_title(title: str) -> Optional[str]:
    """Extract meeting date from FOMC minutes title"""
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']
    
    date_pattern = r'(' + '|'.join(months) + r')\s+\d{1,2}(?:-\d{1,2})?,?\s+\d{4}'
    match = re.search(date_pattern, title)
    
    return match.group(0) if match else None


def extract_date_from_url_fallback(url: str) -> str:
    """Extract date from URL as fallback (YYYYMMDD format)"""
    match = re.search(r'fomcminutes(\d{8})\.htm', url)
    if match:
        date_str = match.group(1)
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:8]
        return f"{year}-{month}-{day}"
    return "Unknown Date"


def calculate_sentiment(text: str) -> Dict[str, float]:
    """Simple VADER-like sentiment calculation"""
    words = text.lower().split()
    
    pos_count = 0
    neg_count = 0
    total_words = len(words)
    
    for word in words:
        if any(pos_word in word for pos_word in POSITIVE_WORDS):
            pos_count += 1
        elif any(neg_word in word for neg_word in NEGATIVE_WORDS):
            neg_count += 1
    
    pos_score = pos_count / total_words if total_words > 0 else 0
    neg_score = neg_count / total_words if total_words > 0 else 0
    neu_score = 1 - (pos_score + neg_score)
    
    compound = (pos_score - neg_score) * 2
    compound = max(-1, min(1, compound))
    
    return {
        'compound': round(compound, 4),
        'pos': round(pos_score, 4),
        'neg': round(neg_score, 4),
        'neu': round(neu_score, 4)
    }


def save_batch_to_s3(data: List[Dict], execution_id: str, start_idx: int, timestamp: datetime) -> str:
    """Save batch data to S3"""
    date_str = timestamp.strftime('%Y-%m-%d')
    
    batch_key = f"bronze/fomc/{date_str}/batch_{execution_id}_{start_idx:04d}.json"
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=batch_key,
        Body=json.dumps(data, indent=2).encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'execution_id': execution_id,
            'batch_start_idx': str(start_idx),
            'records_count': str(len(data))
        }
    )
    
    return batch_key


def consolidate_batches(state: Dict, execution_id: str, timestamp: datetime) -> str:
    """Consolidate all batch files into single final file"""
    print("\n🔄 Consolidating all batches into final file...")
    
    all_data = []
    
    for batch_file in state['batch_files']:
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=batch_file)
            batch_data = json.loads(response['Body'].read().decode('utf-8'))
            all_data.extend(batch_data)
        except Exception as e:
            print(f"  ⚠️ Error reading batch {batch_file}: {e}")
    
    # Sort by date
    all_data.sort(key=lambda x: x.get('date', ''), reverse=True)
    
    # Save final consolidated file
    date_str = timestamp.strftime('%Y-%m-%d')
    datetime_str = timestamp.strftime('%Y%m%d_%H%M%S')
    
    final_key = f"bronze/fomc/{date_str}/fomc_sentiment_complete_{execution_id}.json"
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=final_key,
        Body=json.dumps(all_data, indent=2).encode('utf-8'),
        ContentType='application/json',
        Metadata={
            'execution_id': execution_id,
            'total_records': str(len(all_data)),
            'extraction_complete': 'true'
        }
    )
    
    # Save metadata
    metadata = {
        'extraction_timestamp': timestamp.isoformat(),
        'execution_id': execution_id,
        'source': 'federalreserve_gov',
        'sentiment_method': 'VADER_LITE',
        'total_minutes': len(all_data),
        'successful_count': state['successful_count'],
        'failed_count': len(state['failed_links']),
        'batch_count': len(state['batch_files']),
        'data_file': final_key
    }
    
    metadata_key = f"bronze/fomc/{date_str}/metadata_complete_{execution_id}.json"
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    
    print(f"  ✅ Final file: {final_key}")
    print(f"  ✅ Total records: {len(all_data)}")
    
    return final_key


def create_response(status_code: int, state: Dict, execution_id: str) -> Dict:
    """Create Lambda response"""
    return {
        'statusCode': status_code,
        'body': json.dumps({
            'execution_id': execution_id,
            'status': state['status'],
            'progress': f"{state['processed_count']}/{state['total_links']}",
            'successful': state['successful_count'],
            'failed': len(state['failed_links']),
            'final_file': state.get('final_file'),
            'continue': state['status'] == 'in_progress'
        })
    }