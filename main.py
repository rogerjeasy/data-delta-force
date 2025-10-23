#!/usr/bin/env python3
"""
Main Data Ingestion Script for Macro-Crypto Risk Intelligence Platform.
Windows-compatible version without unicode characters.

Usage Examples:
    python main.py --mode test
    python main.py --mode initial
    python main.py --mode incremental

Authors: Data Delta Force
Created: October 2025
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import json

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

try:
    from data_ingestion.coingecko_client import CoinGeckoClient, CoinGeckoAPIError
    from data_ingestion.fred_client import FREDClient, FREDAPIError
    from data_ingestion.csv_manager import CSVManager
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please ensure all required files are in src/data_ingestion/")
    sys.exit(1)

# Configure logging with UTF-8 encoding for Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class DataIngestionOrchestrator:
    """Orchestrates all data ingestion operations."""
    
    TOP_10_COINS = [
        'bitcoin', 'ethereum', 'tether', 'binancecoin', 'ripple',
        'cardano', 'dogecoin', 'solana', 'polkadot', 'matic-network'
    ]
    
    DEFAULT_MACRO_SERIES = [
        'fed_funds_rate', 'cpi', 'unemployment_rate', 'gdp',
        '10y_treasury', '2y_treasury', 'core_cpi', 'pce'
    ]
    
    def __init__(
        self,
        coingecko_api_key: Optional[str] = None,
        fred_api_key: Optional[str] = None,
        data_dir: str = 'data',
        compression: Optional[str] = None,
        validate_data: bool = True
    ):
        """Initialize orchestrator with API clients and CSV manager."""
        self.data_dir = data_dir
        self.validate_data = validate_data
        
        # Initialize CoinGecko client
        try:
            self.cg_client = CoinGeckoClient(
                api_key=coingecko_api_key,
                validate_data=validate_data
            )
            logger.info("[OK] CoinGecko client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize CoinGecko client: {e}")
            self.cg_client = None
        
        # Initialize FRED client
        try:
            if fred_api_key:
                self.fred_client = FREDClient(
                    api_key=fred_api_key,
                    validate_data=validate_data
                )
                logger.info("[OK] FRED client initialized")
            else:
                self.fred_client = None
                logger.warning("FRED API key not provided")
        except Exception as e:
            logger.error(f"Failed to initialize FRED client: {e}")
            self.fred_client = None
        
        # Initialize CSV manager
        self.csv_manager = CSVManager(
            base_data_dir=data_dir,
            compression=compression,
            create_dirs=True
        )
        logger.info(f"[OK] CSV Manager initialized (data_dir: {data_dir})")
        
        # Execution summary
        self.summary = {
            'start_time': None,
            'end_time': None,
            'crypto_files': [],
            'macro_files': [],
            'errors': []
        }
    
    def fetch_crypto_snapshot(self, coin_ids: Optional[List[str]] = None) -> Optional[str]:
        """Fetch current market snapshot."""
        if not self.cg_client:
            return None
        
        if coin_ids is None:
            coin_ids = self.TOP_10_COINS
        
        logger.info(f"Fetching snapshot for {len(coin_ids)} coins...")
        
        try:
            df = self.cg_client.get_multiple_coins_snapshot(coin_ids)
            if df.empty:
                return None
            
            filepath = self.csv_manager.save_multiple_coins_snapshot(df)
            logger.info(f"[OK] Saved snapshot: {len(df)} coins")
            self.summary['crypto_files'].append(filepath)
            return filepath
        except Exception as e:
            logger.error(f"Snapshot error: {e}")
            self.summary['errors'].append(str(e))
            return None
    
    def fetch_crypto_historical(self, coin_ids: List[str], days: int = 365) -> List[str]:
        """Fetch historical price data."""
        if not self.cg_client:
            return []
        
        logger.info(f"Fetching {days} days historical for {len(coin_ids)} coins...")
        filepaths = []
        
        for i, coin_id in enumerate(coin_ids, 1):
            try:
                logger.info(f"  [{i}/{len(coin_ids)}] {coin_id}...")
                df = self.cg_client.get_historical_prices(coin_id, days=days)
                if not df.empty:
                    filepath = self.csv_manager.save_crypto_data(
                        df, coin_id, 'historical'
                    )
                    logger.info(f"    [OK] Saved {len(df)} records")
                    filepaths.append(filepath)
                    self.summary['crypto_files'].append(filepath)
            except Exception as e:
                logger.error(f"    [ERROR] {e}")
                self.summary['errors'].append(f"{coin_id}: {e}")
        
        return filepaths
    
    def fetch_macro_data(
        self,
        series_names: Optional[List[str]] = None,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None
    ) -> List[str]:
        """Fetch macroeconomic data."""
        if not self.fred_client:
            return []
        
        if series_names is None:
            series_names = self.DEFAULT_MACRO_SERIES
        
        logger.info(f"Fetching {len(series_names)} macro series...")
        filepaths = []
        
        # Fetch each series individually
        for series_name in series_names:
            try:
                series_id = self.fred_client.SERIES_IDS.get(series_name)
                if not series_id:
                    logger.warning(f"Unknown series: {series_name}")
                    continue
                
                df = self.fred_client.get_series(
                    series_id=series_id,
                    observation_start=observation_start,
                    observation_end=observation_end
                )
                
                if df.empty:
                    logger.warning(f"No data for {series_name}")
                    continue
                
                # Determine category
                if series_name in ['fed_funds_rate', '10y_treasury', '2y_treasury']:
                    category = 'interest_rates'
                elif series_name in ['cpi', 'core_cpi', 'pce', 'core_pce']:
                    category = 'inflation'
                elif series_name in ['unemployment_rate', 'nonfarm_payrolls']:
                    category = 'employment'
                elif series_name in ['gdp', 'real_gdp', 'gdp_growth']:
                    category = 'gdp'
                else:
                    category = 'markets'
                
                filepath = self.csv_manager.save_macro_data(
                    df,
                    indicator=series_name,
                    category=category,
                    metadata={
                        'series_id': series_id,
                        'num_observations': len(df)
                    }
                )
                
                logger.info(f"[OK] Saved {series_name} ({len(df)} records)")
                filepaths.append(filepath)
                self.summary['macro_files'].append(filepath)
                
            except Exception as e:
                logger.error(f"Error fetching {series_name}: {e}")
                self.summary['errors'].append(f"{series_name}: {e}")
        
        return filepaths
    
    def run_initial_load(
        self,
        crypto_coins: List[str],
        macro_series: List[str],
        historical_days: int = 365,
        observation_start: Optional[str] = None
    ) -> Dict[str, Any]:
        """Run complete initial data load."""
        logger.info("=" * 80)
        logger.info("INITIAL DATA LOAD")
        logger.info("=" * 80)
        
        self.summary['start_time'] = datetime.utcnow().isoformat()
        
        if crypto_coins:
            self.fetch_crypto_snapshot(crypto_coins)
            self.fetch_crypto_historical(crypto_coins, historical_days)
        
        if macro_series:
            self.fetch_macro_data(macro_series, observation_start)
        
        self.summary['end_time'] = datetime.utcnow().isoformat()
        return self._print_summary()
    
    def run_incremental_update(
        self,
        crypto_coins: Optional[List[str]] = None,
        macro_series: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Run incremental update."""
        logger.info("=" * 80)
        logger.info("INCREMENTAL UPDATE")
        logger.info("=" * 80)
        
        self.summary['start_time'] = datetime.utcnow().isoformat()
        
        if crypto_coins is None:
            crypto_coins = self.TOP_10_COINS
        
        self.fetch_crypto_snapshot(crypto_coins)
        
        if macro_series:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            self.fetch_macro_data(macro_series, start_date)
        
        self.summary['end_time'] = datetime.utcnow().isoformat()
        return self._print_summary()
    
    def run_test_mode(self) -> Dict[str, Any]:
        """Run minimal test."""
        logger.info("=" * 80)
        logger.info("TEST MODE")
        logger.info("=" * 80)
        
        self.summary['start_time'] = datetime.utcnow().isoformat()
        
        if self.cg_client:
            self.fetch_crypto_snapshot(['bitcoin'])
        
        if self.fred_client:
            self.fetch_macro_data(['fed_funds_rate'], '2024-01-01')
        
        self.summary['end_time'] = datetime.utcnow().isoformat()
        return self._print_summary()
    
    def _print_summary(self) -> Dict[str, Any]:
        """Print execution summary."""
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Crypto files: {len(self.summary['crypto_files'])}")
        logger.info(f"Macro files: {len(self.summary['macro_files'])}")
        logger.info(f"Errors: {len(self.summary['errors'])}")
        
        try:
            stats = self.csv_manager.get_storage_stats()
            logger.info(f"\nStorage: {stats['total_size_mb']:.2f} MB")
            logger.info(f"Total files: {stats['total_files']}")
        except:
            pass
        
        if self.summary['errors']:
            logger.info("\nErrors:")
            for error in self.summary['errors']:
                logger.error(f"  - {error}")
        
        return self.summary
    
    def close(self):
        """Close all clients."""
        if self.cg_client:
            self.cg_client.close()
        if self.fred_client:
            self.fred_client.close()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Data Ingestion for Macro-Crypto Risk Intelligence Platform'
    )
    
    parser.add_argument('--mode', type=str, 
                       choices=['initial', 'update', 'incremental', 'test'],
                       required=True, help='Execution mode')
    parser.add_argument('--source', type=str, 
                       choices=['crypto', 'macro', 'both'], default='both')
    parser.add_argument('--coingecko-api-key', type=str, 
                       default=os.getenv('COINGECKO_API_KEY'))
    parser.add_argument('--fred-api-key', type=str, 
                       default=os.getenv('FRED_API_KEY'))
    parser.add_argument('--crypto-coins', type=str,
                       default='bitcoin,ethereum,tether,binancecoin,ripple,cardano,dogecoin,solana,polkadot,matic-network')
    parser.add_argument('--days', type=int, default=365)
    parser.add_argument('--macro-series', type=str,
                       default='fed_funds_rate,cpi,unemployment_rate,gdp,10y_treasury,2y_treasury')
    parser.add_argument('--start-date', type=str, default='2020-01-01')
    parser.add_argument('--data-dir', type=str, default='data')
    parser.add_argument('--compression', type=str, 
                       choices=['gzip', 'bz2', 'none'], default='none')
    parser.add_argument('--no-validation', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    
    return parser.parse_args()


def main():
    """Main execution."""
    args = parse_arguments()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=" * 80)
    logger.info("DATA INGESTION STARTING")
    logger.info("=" * 80)
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Data dir: {args.data_dir}")
    
    # Check environment
    if os.path.exists('.env'):
        logger.info("[OK] Found .env file")
    
    if args.fred_api_key:
        logger.info(f"[OK] FRED API key loaded (length: {len(args.fred_api_key)})")
    
    if args.coingecko_api_key:
        logger.info(f"[OK] CoinGecko API key loaded (length: {len(args.coingecko_api_key)})")
    
    # Parse lists
    crypto_coins = [c.strip() for c in args.crypto_coins.split(',') if c.strip()]
    macro_series = [s.strip() for s in args.macro_series.split(',') if s.strip()]
    
    # Filter by source
    if args.source == 'crypto':
        macro_series = []
    elif args.source == 'macro':
        crypto_coins = []
    
    # Validate FRED key
    if args.source in ['macro', 'both'] and not args.fred_api_key:
        logger.error("=" * 80)
        logger.error("FRED API KEY REQUIRED!")
        logger.error("Please add to .env file: FRED_API_KEY=your_key_here")
        logger.error("=" * 80)
        sys.exit(1)
    
    # Initialize
    compression = None if args.compression == 'none' else args.compression
    
    try:
        orchestrator = DataIngestionOrchestrator(
            coingecko_api_key=args.coingecko_api_key,
            fred_api_key=args.fred_api_key,
            data_dir=args.data_dir,
            compression=compression,
            validate_data=not args.no_validation
        )
        
        # Execute
        if args.mode == 'initial':
            summary = orchestrator.run_initial_load(
                crypto_coins, macro_series, args.days, args.start_date
            )
        elif args.mode == 'update':
            summary = orchestrator.run_initial_load(
                crypto_coins, macro_series, 30, args.start_date
            )
        elif args.mode == 'incremental':
            summary = orchestrator.run_incremental_update(crypto_coins, macro_series)
        elif args.mode == 'test':
            summary = orchestrator.run_test_mode()
        
        # Save summary
        summary_path = Path(args.data_dir) / 'metadata' / f'summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"\n[OK] Summary saved to: {summary_path}")
        
        exit_code = 0 if not summary['errors'] else 1
        
    except KeyboardInterrupt:
        logger.warning("\n[WARN] Interrupted by user")
        exit_code = 130
    except Exception as e:
        logger.error(f"\n[ERROR] Fatal error: {e}", exc_info=True)
        exit_code = 1
    finally:
        try:
            orchestrator.close()
        except:
            pass
    
    logger.info("\n" + "=" * 80)
    logger.info(f"DATA INGESTION COMPLETED (exit code: {exit_code})")
    logger.info("=" * 80)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()