"""
CSV Manager for Data Ingestion Pipeline.

This module handles CSV file operations for the data lake including:
- Writing data with proper schemas
- Partitioning by date and data source
- Metadata tracking
- File compression
- Data append operations

Authors: Data Delta Force
Created: October 2025
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
import pandas as pd
import json

logger = logging.getLogger(__name__)


class CSVManager:
    """
    Manager for CSV file operations in the data lake.
    
    Handles:
    - Structured directory organization
    - Consistent file naming
    - Schema enforcement
    - Metadata tracking
    - Incremental updates
    """
    
    def __init__(
        self,
        base_data_dir: str = "data",
        compression: Optional[str] = None,
        create_dirs: bool = True
    ):
        """
        Initialize CSV Manager.
        
        Args:
            base_data_dir: Base directory for all data storage
            compression: Compression type ('gzip', 'bz2', 'zip', 'xz', None)
            create_dirs: Automatically create directory structure
            
        Example:
            >>> manager = CSVManager(base_data_dir="data")
            >>> manager.save_crypto_data(df, "bitcoin", "prices")
        """
        self.base_data_dir = Path(base_data_dir)
        self.compression = compression
        
        # Define directory structure
        self.dirs = {
            'raw_crypto': self.base_data_dir / 'raw' / 'crypto',
            'raw_macro': self.base_data_dir / 'raw' / 'macro',
            'metadata': self.base_data_dir / 'metadata',
            'processed': self.base_data_dir / 'processed'
        }
        
        if create_dirs:
            self._create_directory_structure()
        
        logger.info(f"CSVManager initialized with base directory: {self.base_data_dir}")
    
    def _create_directory_structure(self) -> None:
        """Create the directory structure for data storage."""
        # Main directories
        for dir_path in self.dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Crypto subdirectories
        crypto_dirs = ['prices', 'market_data', 'sentiment', 'historical']
        for subdir in crypto_dirs:
            (self.dirs['raw_crypto'] / subdir).mkdir(parents=True, exist_ok=True)
        
        # Macro subdirectories
        macro_dirs = ['interest_rates', 'inflation', 'employment', 'gdp', 'markets']
        for subdir in macro_dirs:
            (self.dirs['raw_macro'] / subdir).mkdir(parents=True, exist_ok=True)
        
        logger.info("Directory structure created successfully")
    
    def _generate_filename(
        self,
        source: str,
        asset_or_indicator: str,
        data_type: str,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Generate standardized filename.
        
        Format: {source}_{asset}_{type}_{YYYYMMDD}_{HHMMSS}.csv[.gz]
        
        Args:
            source: Data source ('coingecko', 'fred')
            asset_or_indicator: Asset name or indicator
            data_type: Type of data (prices, market_data, etc.)
            timestamp: Timestamp for filename (uses current time if None)
            
        Returns:
            Filename string
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        date_str = timestamp.strftime('%Y%m%d')
        time_str = timestamp.strftime('%H%M%S')
        
        # Sanitize asset/indicator name
        asset_clean = asset_or_indicator.lower().replace(' ', '_').replace('-', '_')
        
        filename = f"{source}_{asset_clean}_{data_type}_{date_str}_{time_str}.csv"
        
        if self.compression:
            filename += f".{self.compression}"
        
        return filename
    
    def save_crypto_data(
        self,
        df: pd.DataFrame,
        coin_id: str,
        data_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Save cryptocurrency data to CSV.
        
        Args:
            df: DataFrame with crypto data
            coin_id: Cryptocurrency identifier
            data_type: Type of data (prices, market_data, sentiment)
            metadata: Optional metadata dictionary
            
        Returns:
            Path to saved file
            
        Example:
            >>> df = pd.DataFrame({'price': [45000, 45100], ...})
            >>> path = manager.save_crypto_data(df, 'bitcoin', 'prices')
        """
        # Determine subdirectory
        if data_type in ['prices', 'historical']:
            subdir = self.dirs['raw_crypto'] / 'prices'
        elif data_type in ['market_data', 'snapshot']:
            subdir = self.dirs['raw_crypto'] / 'market_data'
        elif data_type in ['sentiment', 'social']:
            subdir = self.dirs['raw_crypto'] / 'sentiment'
        else:
            subdir = self.dirs['raw_crypto']
        
        # Generate filename
        filename = self._generate_filename('coingecko', coin_id, data_type)
        filepath = subdir / filename
        
        # Add fetch metadata to dataframe
        df_to_save = df.copy()
        if 'fetch_datetime' not in df_to_save.columns:
            df_to_save['fetch_datetime'] = datetime.utcnow()
        if 'data_source' not in df_to_save.columns:
            df_to_save['data_source'] = 'coingecko'
        
        # Save to CSV
        df_to_save.to_csv(
            filepath,
            index=False,
            compression=self.compression
        )
        
        # Save metadata
        if metadata:
            self._save_metadata(filepath, metadata, 'crypto', coin_id, data_type)
        
        logger.info(f"Saved crypto data to {filepath} ({len(df)} rows)")
        return str(filepath)
    
    def save_macro_data(
        self,
        df: pd.DataFrame,
        indicator: str,
        category: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Save macroeconomic data to CSV.
        
        Args:
            df: DataFrame with macro data
            indicator: Economic indicator name
            category: Category (interest_rates, inflation, employment, gdp, markets)
            metadata: Optional metadata dictionary
            
        Returns:
            Path to saved file
            
        Example:
            >>> df = pd.DataFrame({'date': [...], 'value': [...]})
            >>> path = manager.save_macro_data(df, 'cpi', 'inflation')
        """
        # Determine subdirectory
        if category in ['interest_rates', 'inflation', 'employment', 'gdp', 'markets']:
            subdir = self.dirs['raw_macro'] / category
        else:
            subdir = self.dirs['raw_macro']
        
        # Generate filename
        filename = self._generate_filename('fred', indicator, category)
        filepath = subdir / filename
        
        # Add fetch metadata
        df_to_save = df.copy()
        if 'fetch_datetime' not in df_to_save.columns:
            df_to_save['fetch_datetime'] = datetime.utcnow()
        if 'data_source' not in df_to_save.columns:
            df_to_save['data_source'] = 'fred'
        
        # Save to CSV
        df_to_save.to_csv(
            filepath,
            index=False,
            compression=self.compression
        )
        
        # Save metadata
        if metadata:
            self._save_metadata(filepath, metadata, 'macro', indicator, category)
        
        logger.info(f"Saved macro data to {filepath} ({len(df)} rows)")
        return str(filepath)
    
    def save_multiple_coins_snapshot(
        self,
        df: pd.DataFrame,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Save multi-coin market snapshot.
        
        Args:
            df: DataFrame with multiple coins data
            metadata: Optional metadata dictionary
            
        Returns:
            Path to saved file
        """
        filename = self._generate_filename('coingecko', 'multi_coin', 'snapshot')
        filepath = self.dirs['raw_crypto'] / 'market_data' / filename
        
        # Add metadata
        df_to_save = df.copy()
        if 'fetch_datetime' not in df_to_save.columns:
            df_to_save['fetch_datetime'] = datetime.utcnow()
        if 'data_source' not in df_to_save.columns:
            df_to_save['data_source'] = 'coingecko'
        
        # Save
        df_to_save.to_csv(filepath, index=False, compression=self.compression)
        
        # Save metadata
        if metadata is None:
            metadata = {}
        metadata['num_coins'] = len(df)
        metadata['coins'] = df['coin_id'].tolist() if 'coin_id' in df.columns else []
        
        self._save_metadata(filepath, metadata, 'crypto', 'multi_coin', 'snapshot')
        
        logger.info(f"Saved multi-coin snapshot to {filepath} ({len(df)} coins)")
        return str(filepath)
    
    def save_multiple_macro_series(
        self,
        df: pd.DataFrame,
        series_names: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Save multiple macro series in one file.
        
        Args:
            df: DataFrame with multiple series
            series_names: List of series names
            metadata: Optional metadata dictionary
            
        Returns:
            Path to saved file
        """
        filename = self._generate_filename('fred', 'multi_series', 'combined')
        filepath = self.dirs['raw_macro'] / filename
        
        # Add metadata
        df_to_save = df.copy()
        if 'fetch_datetime' not in df_to_save.columns:
            df_to_save['fetch_datetime'] = datetime.utcnow()
        if 'data_source' not in df_to_save.columns:
            df_to_save['data_source'] = 'fred'
        
        # Save
        df_to_save.to_csv(filepath, index=False, compression=self.compression)
        
        # Save metadata
        if metadata is None:
            metadata = {}
        metadata['num_series'] = len(series_names)
        metadata['series_names'] = series_names
        
        self._save_metadata(filepath, metadata, 'macro', 'multi_series', 'combined')
        
        logger.info(f"Saved multi-series macro data to {filepath} ({len(series_names)} series)")
        return str(filepath)
    
    def _save_metadata(
        self,
        data_filepath: Path,
        metadata: Dict[str, Any],
        source_type: str,
        identifier: str,
        data_type: str
    ) -> None:
        """
        Save metadata about a data fetch operation.
        
        Args:
            data_filepath: Path to the data file
            metadata: Metadata dictionary
            source_type: 'crypto' or 'macro'
            identifier: Asset/indicator identifier
            data_type: Type of data
        """
        metadata_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'data_filepath': str(data_filepath),
            'source_type': source_type,
            'identifier': identifier,
            'data_type': data_type,
            'file_size_bytes': data_filepath.stat().st_size if data_filepath.exists() else 0,
            **metadata
        }
        
        # Append to metadata log
        metadata_log_path = self.dirs['metadata'] / 'fetch_logs.csv'
        
        # Convert to DataFrame
        metadata_df = pd.DataFrame([metadata_entry])
        
        # Append or create
        if metadata_log_path.exists():
            metadata_df.to_csv(
                metadata_log_path,
                mode='a',
                header=False,
                index=False
            )
        else:
            metadata_df.to_csv(
                metadata_log_path,
                mode='w',
                header=True,
                index=False
            )
        
        logger.debug(f"Metadata saved to {metadata_log_path}")
    
    def append_to_existing(
        self,
        new_df: pd.DataFrame,
        existing_file: str,
        deduplicate: bool = True,
        dedupe_columns: Optional[List[str]] = None
    ) -> str:
        """
        Append new data to existing CSV file.
        
        Args:
            new_df: New data to append
            existing_file: Path to existing file
            deduplicate: Remove duplicates after appending
            dedupe_columns: Columns to use for deduplication
            
        Returns:
            Path to updated file
        """
        existing_path = Path(existing_file)
        
        if not existing_path.exists():
            logger.warning(f"File {existing_file} does not exist, creating new file")
            new_df.to_csv(existing_path, index=False, compression=self.compression)
            return str(existing_path)
        
        # Read existing data
        existing_df = pd.read_csv(existing_path, compression='infer')
        
        # Combine
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Deduplicate if requested
        if deduplicate:
            if dedupe_columns:
                combined_df = combined_df.drop_duplicates(subset=dedupe_columns, keep='last')
            else:
                combined_df = combined_df.drop_duplicates(keep='last')
        
        # Save
        combined_df.to_csv(existing_path, index=False, compression=self.compression)
        
        logger.info(
            f"Appended {len(new_df)} rows to {existing_file} "
            f"(total: {len(combined_df)} rows)"
        )
        
        return str(existing_path)
    
    def get_latest_file(
        self,
        source_type: str,
        identifier: str,
        data_type: str
    ) -> Optional[str]:
        """
        Get the most recent file for given parameters.
        
        Args:
            source_type: 'crypto' or 'macro'
            identifier: Asset/indicator identifier
            data_type: Type of data
            
        Returns:
            Path to latest file or None
        """
        # Determine search directory
        if source_type == 'crypto':
            if data_type in ['prices', 'historical']:
                search_dir = self.dirs['raw_crypto'] / 'prices'
            else:
                search_dir = self.dirs['raw_crypto'] / 'market_data'
        else:  # macro
            search_dir = self.dirs['raw_macro']
        
        # Search for matching files
        pattern = f"*_{identifier}_{data_type}_*.csv*"
        matching_files = list(search_dir.glob(pattern))
        
        if not matching_files:
            return None
        
        # Sort by modification time
        latest_file = max(matching_files, key=lambda p: p.stat().st_mtime)
        
        return str(latest_file)
    
    def get_metadata_summary(self) -> pd.DataFrame:
        """
        Get summary of all fetch operations from metadata log.
        
        Returns:
            DataFrame with metadata summary
        """
        metadata_log_path = self.dirs['metadata'] / 'fetch_logs.csv'
        
        if not metadata_log_path.exists():
            logger.warning("No metadata log found")
            return pd.DataFrame()
        
        df = pd.read_csv(metadata_log_path)
        
        return df
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get statistics about data storage.
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {
            'total_files': 0,
            'total_size_mb': 0,
            'crypto_files': 0,
            'macro_files': 0,
            'by_type': {}
        }
        
        # Count files and sizes
        for dir_name, dir_path in self.dirs.items():
            if 'raw' in dir_name:
                files = list(dir_path.rglob('*.csv*'))
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                
                stats['total_files'] += len(files)
                stats['total_size_mb'] += total_size / (1024 * 1024)
                
                if 'crypto' in dir_name:
                    stats['crypto_files'] += len(files)
                elif 'macro' in dir_name:
                    stats['macro_files'] += len(files)
                
                stats['by_type'][dir_name] = {
                    'files': len(files),
                    'size_mb': total_size / (1024 * 1024)
                }
        
        return stats