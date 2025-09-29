"""
Data Validation Module for API Responses.

Provides comprehensive validation for data collected from external APIs:
- Schema validation
- Type checking
- Range validation
- Missing value detection
- Outlier detection
- Data quality metrics

Authors: Data Delta Force
Created: September 2025
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime
from dataclasses import dataclass
import pandas as pd
import numpy as np
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """
    Result of a validation check.
    
    Attributes:
        is_valid: Whether validation passed
        field_name: Name of validated field
        severity: Severity level of any issues
        message: Description of validation result
        invalid_values: List of invalid values found
        metadata: Additional context information
    """
    is_valid: bool
    field_name: str
    severity: ValidationSeverity
    message: str
    invalid_values: Optional[List[Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        status = "PASS" if self.is_valid else "FAIL"
        return (f"[{self.severity.value.upper()}] {self.field_name}: "
                f"{status} - {self.message}")


class DataValidator:
    """
    Comprehensive data validator for API responses.
    
    Supports validation rules for:
    - Cryptocurrency market data
    - Macroeconomic indicators
    - Time series data
    - Custom validation rules
    """
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize data validator.
        
        Args:
            strict_mode: If True, any validation error raises exception.
                        If False, logs errors and returns validation results.
        """
        self.strict_mode = strict_mode
        self.validation_history: List[ValidationResult] = []
        
    def validate_crypto_data(
        self, 
        data: Dict[str, Any],
        required_fields: Optional[List[str]] = None
    ) -> List[ValidationResult]:
        """
        Validate cryptocurrency market data.
        
        Args:
            data: Dictionary containing crypto data
            required_fields: List of required field names
            
        Returns:
            List of validation results
            
        Example:
            >>> validator = DataValidator()
            >>> results = validator.validate_crypto_data({
            ...     'id': 'bitcoin',
            ...     'current_price': 45000.50,
            ...     'market_cap': 850000000000,
            ...     'total_volume': 25000000000
            ... })
        """
        if required_fields is None:
            required_fields = [
                'id', 'symbol', 'name', 'current_price', 
                'market_cap', 'total_volume'
            ]
        
        results = []
        
        # Check required fields
        results.extend(self._validate_required_fields(data, required_fields))
        
        # Validate price fields
        if 'current_price' in data:
            results.append(self._validate_numeric_range(
                data.get('current_price'),
                'current_price',
                min_value=0,
                max_value=1_000_000
            ))
        
        # Validate market cap
        if 'market_cap' in data:
            results.append(self._validate_numeric_range(
                data.get('market_cap'),
                'market_cap',
                min_value=0,
                max_value=10_000_000_000_000  # 10 trillion
            ))
        
        # Validate volume
        if 'total_volume' in data:
            results.append(self._validate_numeric_range(
                data.get('total_volume'),
                'total_volume',
                min_value=0,
                max_value=1_000_000_000_000  # 1 trillion
            ))
        
        # Validate percentage changes
        for field in ['price_change_percentage_24h', 'price_change_percentage_7d']:
            if field in data:
                results.append(self._validate_numeric_range(
                    data.get(field),
                    field,
                    min_value=-100,
                    max_value=1000
                ))
        
        # Validate timestamps
        if 'last_updated' in data:
            results.append(self._validate_timestamp(
                data.get('last_updated'),
                'last_updated'
            ))
        
        self._log_and_store_results(results)
        return results
    
    def validate_macro_data(
        self,
        data: Dict[str, Any],
        indicator_type: str
    ) -> List[ValidationResult]:
        """
        Validate macroeconomic indicator data.
        
        Args:
            data: Dictionary containing macro data
            indicator_type: Type of indicator (e.g., 'inflation', 'employment')
            
        Returns:
            List of validation results
        """
        results = []
        
        # Common validations
        results.extend(self._validate_required_fields(
            data, 
            ['date', 'value']
        ))
        
        # Validate date
        if 'date' in data:
            results.append(self._validate_date_format(
                data.get('date'),
                'date'
            ))
        
        # Validate value based on indicator type
        if 'value' in data:
            value = data.get('value')
            
            if indicator_type == 'inflation':
                results.append(self._validate_numeric_range(
                    value,
                    'value',
                    min_value=-20.0,
                    max_value=50.0,
                    severity=ValidationSeverity.WARNING
                ))
            elif indicator_type == 'interest_rate':
                results.append(self._validate_numeric_range(
                    value,
                    'value',
                    min_value=-5.0,
                    max_value=25.0
                ))
            elif indicator_type == 'unemployment':
                results.append(self._validate_numeric_range(
                    value,
                    'value',
                    min_value=0.0,
                    max_value=30.0
                ))
            else:
                results.append(self._validate_not_null(value, 'value'))
        
        self._log_and_store_results(results)
        return results
    
    def validate_time_series(
        self,
        df: pd.DataFrame,
        timestamp_col: str = 'timestamp',
        value_col: str = 'value'
    ) -> List[ValidationResult]:
        """
        Validate time series data in DataFrame.
        
        Args:
            df: DataFrame containing time series data
            timestamp_col: Name of timestamp column
            value_col: Name of value column
            
        Returns:
            List of validation results
        """
        results = []
        
        # Check DataFrame is not empty
        if df.empty:
            results.append(ValidationResult(
                is_valid=False,
                field_name='dataframe',
                severity=ValidationSeverity.ERROR,
                message="DataFrame is empty"
            ))
            return results
        
        # Validate required columns exist
        if timestamp_col not in df.columns:
            results.append(ValidationResult(
                is_valid=False,
                field_name=timestamp_col,
                severity=ValidationSeverity.ERROR,
                message=f"Timestamp column '{timestamp_col}' not found"
            ))
        
        if value_col not in df.columns:
            results.append(ValidationResult(
                is_valid=False,
                field_name=value_col,
                severity=ValidationSeverity.ERROR,
                message=f"Value column '{value_col}' not found"
            ))
            return results
        
        # Check for missing values
        missing_values = df[value_col].isna().sum()
        if missing_values > 0:
            missing_pct = (missing_values / len(df)) * 100
            results.append(ValidationResult(
                is_valid=missing_pct < 10,
                field_name=value_col,
                severity=ValidationSeverity.WARNING if missing_pct < 10 else ValidationSeverity.ERROR,
                message=f"Found {missing_values} missing values ({missing_pct:.2f}%)",
                metadata={'missing_count': missing_values, 'missing_percentage': missing_pct}
            ))
        
        # Check for duplicates
        duplicates = df[timestamp_col].duplicated().sum()
        if duplicates > 0:
            results.append(ValidationResult(
                is_valid=False,
                field_name=timestamp_col,
                severity=ValidationSeverity.WARNING,
                message=f"Found {duplicates} duplicate timestamps",
                metadata={'duplicate_count': duplicates}
            ))
        
        # Check time series is sorted
        if timestamp_col in df.columns:
            is_sorted = df[timestamp_col].is_monotonic_increasing
            results.append(ValidationResult(
                is_valid=is_sorted,
                field_name=timestamp_col,
                severity=ValidationSeverity.INFO,
                message="Time series is sorted" if is_sorted else "Time series is not sorted"
            ))
        
        # Detect outliers using IQR method
        if pd.api.types.is_numeric_dtype(df[value_col]):
            outliers = self._detect_outliers_iqr(df[value_col])
            if len(outliers) > 0:
                outlier_pct = (len(outliers) / len(df)) * 100
                results.append(ValidationResult(
                    is_valid=outlier_pct < 5,
                    field_name=value_col,
                    severity=ValidationSeverity.WARNING,
                    message=f"Detected {len(outliers)} outliers ({outlier_pct:.2f}%)",
                    invalid_values=outliers[:10],  # First 10 outliers
                    metadata={'outlier_count': len(outliers), 'outlier_percentage': outlier_pct}
                ))
        
        self._log_and_store_results(results)
        return results
    
    def _validate_required_fields(
        self,
        data: Dict[str, Any],
        required_fields: List[str]
    ) -> List[ValidationResult]:
        """Validate presence of required fields."""
        results = []
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            results.append(ValidationResult(
                is_valid=False,
                field_name='required_fields',
                severity=ValidationSeverity.ERROR,
                message=f"Missing required fields: {', '.join(missing_fields)}",
                invalid_values=missing_fields
            ))
        else:
            results.append(ValidationResult(
                is_valid=True,
                field_name='required_fields',
                severity=ValidationSeverity.INFO,
                message="All required fields present"
            ))
        
        return results
    
    def _validate_numeric_range(
        self,
        value: Any,
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ) -> ValidationResult:
        """Validate numeric value is within range."""
        if value is None:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                severity=severity,
                message=f"{field_name} is None"
            )
        
        try:
            numeric_value = float(value)
            
            if min_value is not None and numeric_value < min_value:
                return ValidationResult(
                    is_valid=False,
                    field_name=field_name,
                    severity=severity,
                    message=f"{field_name} ({numeric_value}) below minimum ({min_value})",
                    invalid_values=[numeric_value]
                )
            
            if max_value is not None and numeric_value > max_value:
                return ValidationResult(
                    is_valid=False,
                    field_name=field_name,
                    severity=severity,
                    message=f"{field_name} ({numeric_value}) above maximum ({max_value})",
                    invalid_values=[numeric_value]
                )
            
            return ValidationResult(
                is_valid=True,
                field_name=field_name,
                severity=ValidationSeverity.INFO,
                message=f"{field_name} within valid range"
            )
            
        except (ValueError, TypeError) as e:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                severity=ValidationSeverity.ERROR,
                message=f"{field_name} is not numeric: {str(e)}",
                invalid_values=[value]
            )
    
    def _validate_timestamp(
        self,
        value: Any,
        field_name: str
    ) -> ValidationResult:
        """Validate timestamp is reasonable."""
        if value is None:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                severity=ValidationSeverity.ERROR,
                message=f"{field_name} is None"
            )
        
        try:
            if isinstance(value, str):
                timestamp = pd.to_datetime(value)
            elif isinstance(value, (int, float)):
                timestamp = pd.to_datetime(value, unit='ms')
            else:
                timestamp = pd.to_datetime(value)
            
            # Check timestamp is not in the future
            if timestamp > pd.Timestamp.now():
                return ValidationResult(
                    is_valid=False,
                    field_name=field_name,
                    severity=ValidationSeverity.WARNING,
                    message=f"{field_name} is in the future: {timestamp}"
                )
            
            # Check timestamp is not too old (e.g., before 2009 - Bitcoin genesis)
            if timestamp < pd.Timestamp('2009-01-01'):
                return ValidationResult(
                    is_valid=False,
                    field_name=field_name,
                    severity=ValidationSeverity.WARNING,
                    message=f"{field_name} is suspiciously old: {timestamp}"
                )
            
            return ValidationResult(
                is_valid=True,
                field_name=field_name,
                severity=ValidationSeverity.INFO,
                message=f"{field_name} is valid"
            )
            
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                severity=ValidationSeverity.ERROR,
                message=f"Invalid timestamp format: {str(e)}",
                invalid_values=[value]
            )
    
    def _validate_date_format(
        self,
        value: Any,
        field_name: str
    ) -> ValidationResult:
        """Validate date string format."""
        if value is None:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                severity=ValidationSeverity.ERROR,
                message=f"{field_name} is None"
            )
        
        try:
            date = pd.to_datetime(value)
            return ValidationResult(
                is_valid=True,
                field_name=field_name,
                severity=ValidationSeverity.INFO,
                message=f"{field_name} has valid format"
            )
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                severity=ValidationSeverity.ERROR,
                message=f"Invalid date format: {str(e)}",
                invalid_values=[value]
            )
    
    def _validate_not_null(
        self,
        value: Any,
        field_name: str
    ) -> ValidationResult:
        """Validate value is not null."""
        is_valid = value is not None and value != ""
        
        return ValidationResult(
            is_valid=is_valid,
            field_name=field_name,
            severity=ValidationSeverity.ERROR if not is_valid else ValidationSeverity.INFO,
            message=f"{field_name} is {'valid' if is_valid else 'null or empty'}"
        )
    
    def _detect_outliers_iqr(
        self,
        series: pd.Series,
        multiplier: float = 1.5
    ) -> List[float]:
        """
        Detect outliers using Interquartile Range method.
        
        Args:
            series: Pandas Series of numeric values
            multiplier: IQR multiplier (default 1.5 for standard outliers)
            
        Returns:
            List of outlier values
        """
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        return outliers.tolist()
    
    def _log_and_store_results(
        self,
        results: List[ValidationResult]
    ) -> None:
        """Log and store validation results."""
        self.validation_history.extend(results)
        
        for result in results:
            if result.severity == ValidationSeverity.ERROR:
                logger.error(str(result))
                if self.strict_mode and not result.is_valid:
                    raise ValueError(f"Validation failed: {result.message}")
            elif result.severity == ValidationSeverity.WARNING:
                logger.warning(str(result))
            elif result.severity == ValidationSeverity.CRITICAL:
                logger.critical(str(result))
                if self.strict_mode:
                    raise ValueError(f"Critical validation failure: {result.message}")
            else:
                logger.info(str(result))
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get summary of all validation results.
        
        Returns:
            Dictionary with validation statistics
        """
        if not self.validation_history:
            return {'total_validations': 0}
        
        severity_counts = {
            severity: sum(1 for r in self.validation_history if r.severity == severity)
            for severity in ValidationSeverity
        }
        
        passed = sum(1 for r in self.validation_history if r.is_valid)
        failed = sum(1 for r in self.validation_history if not r.is_valid)
        
        return {
            'total_validations': len(self.validation_history),
            'passed': passed,
            'failed': failed,
            'success_rate': (passed / len(self.validation_history)) * 100,
            'severity_breakdown': {s.value: c for s, c in severity_counts.items()}
        }
    
    def clear_history(self) -> None:
        """Clear validation history."""
        self.validation_history.clear()
        logger.info("Validation history cleared")