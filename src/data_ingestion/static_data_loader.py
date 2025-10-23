
"""
Static Data Loader Module
==========================

Loads and provides access to static reference data sources:
- Market regime classifications
- Exchange listing dates (coming soon)

Usage:
    from src.data_ingestion.static_data_loader import StaticDataLoader

    loader = StaticDataLoader()
    regimes = loader.load_market_regimes()
    print(regimes.head())
"""

import pandas as pd
import os
from pathlib import Path
from typing import Optional

class StaticDataLoader:
    """Load and manage static reference data"""

    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize StaticDataLoader

        Args:
            data_dir: Base directory for data files.
                     If None, uses Project/data/ relative to this file
        """
        if data_dir is None:
            # Get project root (go up from src/data_ingestion/ to Project/)
            current_file = Path(__file__)
            project_root = current_file.parent.parent.parent
            self.data_dir = project_root / 'data' / 'raw'
        else:
            self.data_dir = Path(data_dir)

        self.market_regimes = None
        self.exchange_listings = None

    def load_market_regimes(self, reload: bool = False) -> pd.DataFrame:
        """
        Load historical market regime classifications

        Args:
            reload: If True, reload data even if already cached

        Returns:
            DataFrame with columns:
            - date: Trading date
            - sp500_close: S&P 500 closing price
            - vix_close: VIX closing value
            - drawdown: Drawdown from peak (%)
            - bull_bear_regime: Bull or Bear market
            - vix_regime: Low/Medium/High volatility
            - market_regime: Combined regime classification

        Raises:
            FileNotFoundError: If market_regimes.csv not found
        """
        if self.market_regimes is not None and not reload:
            return self.market_regimes

        filepath = self.data_dir / 'market_regimes.csv'

        if not filepath.exists():
            raise FileNotFoundError(
                f"Market regimes file not found: {filepath}\n"
                f"Please run: python scripts/generate_market_regimes.py"
            )

        self.market_regimes = pd.read_csv(filepath, parse_dates=['date'])
        print(f"✅ Loaded {len(self.market_regimes)} days of market regime data")
        print \
            (f"   Date range: {self.market_regimes['date'].min().date()} to {self.market_regimes['date'].max().date()}")

        return self.market_regimes

    def get_regime_for_date(self, date: str) -> Optional[dict]:
        """
        Get market regime for a specific date

        Args:
            date: Date string in format 'YYYY-MM-DD'

        Returns:
            Dictionary with regime information for that date, or None if not found

        Example:
            >>> loader = StaticDataLoader()
            >>> regime = loader.get_regime_for_date('2020-03-15')
            >>> print(regime['market_regime'])
        """
        if self.market_regimes is None:
            self.load_market_regimes()

        date_obj = pd.to_datetime(date)
        row = self.market_regimes[self.market_regimes['date'] == date_obj]

        if len(row) == 0:
            return None

        return row.iloc[0].to_dict()

    def get_regime_statistics(self) -> pd.DataFrame:
        """
        Get summary statistics for each market regime

        Returns:
            DataFrame with regime counts and percentages

        Example:
            >>> loader = StaticDataLoader()
            >>> stats = loader.get_regime_statistics()
            >>> print(stats)
        """
        if self.market_regimes is None:
            self.load_market_regimes()

        stats = self.market_regimes['market_regime'].value_counts()
        stats_df = pd.DataFrame({
            'regime': stats.index,
            'days': stats.values,
            'percentage': (stats.values / len(self.market_regimes) * 100).round(2)
        })

        return stats_df

    def filter_by_regime(self, regime: str) -> pd.DataFrame:
        """
        Filter data to specific market regime

        Args:
            regime: Market regime to filter
                   Options: 'Bull_Low_Vol', 'Bull_High_Vol',
                           'Bear_High_Vol', 'Bear_Medium_Vol'

        Returns:
            DataFrame filtered to specified regime

        Example:
            >>> loader = StaticDataLoader()
            >>> bear_market = loader.filter_by_regime('Bear_High_Vol')
            >>> print(f"Bear market days: {len(bear_market)}")
        """
        if self.market_regimes is None:
            self.load_market_regimes()

        return self.market_regimes[self.market_regimes['market_regime'] == regime].copy()

    def get_bull_bear_periods(self) -> list:
        """
        Identify continuous bull and bear market periods

        Returns:
            List of dicts with start_date, end_date, regime, duration_days

        Example:
            >>> loader = StaticDataLoader()
            >>> periods = loader.get_bull_bear_periods()
            >>> for p in periods[:3]:
            ...     print(f"{p['regime']}: {p['start_date']} to {p['end_date']} ({p['duration_days']} days)")
        """
        if self.market_regimes is None:
            self.load_market_regimes()

        data = self.market_regimes.copy()
        data['regime_change'] = data['bull_bear_regime'] != data['bull_bear_regime'].shift(1)
        data['period_id'] = data['regime_change'].cumsum()

        periods = []
        for period_id in data['period_id'].unique():
            period_data = data[data['period_id'] == period_id]
            periods.append({
                'start_date': period_data['date'].min(),
                'end_date': period_data['date'].max(),
                'regime': period_data['bull_bear_regime'].iloc[0],
                'duration_days': len(period_data)
            })

        return periods

    def load_exchange_listings(self, reload: bool = False) -> pd.DataFrame:
        """
        Load cryptocurrency exchange listing dates

        Args:
            reload: If True, reload data even if already cached

        Returns:
            DataFrame with columns: symbol, exchange, listing_date

        Raises:
            FileNotFoundError: If exchange_listings.csv not found
        """
        if self.exchange_listings is not None and not reload:
            return self.exchange_listings

        filepath = self.data_dir / 'exchange_listings.csv'

        if not filepath.exists():
            raise FileNotFoundError(
                f"Exchange listings file not found: {filepath}\n"
                f"Please run: python scripts/create_exchange_listings.py"
            )

        self.exchange_listings = pd.read_csv(filepath, parse_dates=['listing_date'])
        print(f"✅ Loaded {len(self.exchange_listings)} exchange listings")

        return self.exchange_listings

if __name__ == "__main__":
    # Example usage
    print("=" * 60)
    print("Testing StaticDataLoader")
    print("=" * 60)

    loader = StaticDataLoader()

    # Load market regimes
    print("\n1. Loading market regimes...")
    regimes = loader.load_market_regimes()
    print(regimes.head())

    # Get statistics
    print("\n2. Regime statistics:")
    stats = loader.get_regime_statistics()
    print(stats)

    # Get specific date
    print("\n3. Get regime for specific date (2020-03-15 - COVID crash):")
    regime = loader.get_regime_for_date('2020-03-15')
    if regime:
        print(f"   Regime: {regime['market_regime']}")
        print(f"   S&P 500: {regime['sp500_close']:.2f}")
        print(f"   VIX: {regime['vix_close']:.2f}")
        print(f"   Drawdown: {regime['drawdown']:.2f}%")

    # Bull/Bear periods
    print("\n4. Bull/Bear market periods:")
    periods = loader.get_bull_bear_periods()
    for p in periods[:5]:
        print(f"   {p['regime']}: {p['start_date'].date()} to {p['end_date'].date()} ({p['duration_days']} days)")

    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)