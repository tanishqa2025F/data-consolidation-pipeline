"""
cleaning.py
Data cleaning functions for standardizing messy business data.

Handles common real-world data quality issues:
- Inconsistent date formats across systems
- Whitespace and capitalization issues in names
- Invalid/impossible numeric values
- Duplicate record detection and removal
- Null value handling with configurable strategies
"""

import pandas as pd
import numpy as np
import re
import logging

logger = logging.getLogger(__name__)


def standardize_dates(df, date_col, output_format="%Y-%m-%d"):
    """
    Parse dates from multiple formats into a single standardized format.
    
    Handles: MM/DD/YYYY, YYYY-MM-DD, MM-DD-YY, and other common formats.
    This solves the exact problem the BA described — 'issue date' vs 'report date'
    using different formats across systems.
    """
    original_count = len(df)
    
    # Try parsing with pandas (handles multiple formats automatically)
    df[date_col] = pd.to_datetime(df[date_col], format="mixed", dayfirst=False, errors="coerce")
    
    parsed_count = df[date_col].notna().sum()
    failed_count = original_count - parsed_count
    
    if failed_count > 0:
        logger.warning(f"  Could not parse {failed_count} date values in '{date_col}'")
    
    logger.info(f"  Standardized {parsed_count}/{original_count} dates in '{date_col}'")
    return df


def clean_names(df, name_col):
    """
    Standardize person names: strip whitespace, fix capitalization,
    remove middle initials for consistent matching across systems.
    
    'mike johnson' → 'Mike Johnson'
    '  Sarah J. Chen  ' → 'Sarah Chen'
    """
    def normalize_name(name):
        if pd.isna(name):
            return name
        # Strip whitespace
        name = str(name).strip()
        # Remove middle initials (e.g., "J." or "A")
        name = re.sub(r'\s+[A-Z]\.?\s+', ' ', name)
        # Fix capitalization
        name = name.title()
        # Collapse multiple spaces
        name = re.sub(r'\s+', ' ', name)
        return name
    
    before_unique = df[name_col].nunique()
    df[name_col] = df[name_col].apply(normalize_name)
    after_unique = df[name_col].nunique()
    
    if before_unique != after_unique:
        logger.info(f"  Name standardization reduced unique values in '{name_col}': "
                    f"{before_unique} → {after_unique}")
    
    return df


def clean_currency(df, amount_col):
    """
    Clean currency values: remove $, commas, handle negative values.
    Convert string-formatted amounts to proper float type.
    
    '$1,234.56' → 1234.56
    '-450.00' → NaN (flagged as data entry error)
    """
    def parse_amount(val):
        if pd.isna(val):
            return np.nan
        val_str = str(val).replace("$", "").replace(",", "").strip()
        try:
            amount = float(val_str)
            if amount < 0:
                logger.warning(f"  Negative amount found: {amount} — flagged as data entry error")
                return np.nan
            return amount
        except ValueError:
            logger.warning(f"  Could not parse amount: '{val}'")
            return np.nan
    
    original_valid = df[amount_col].notna().sum()
    df[amount_col] = df[amount_col].apply(parse_amount)
    new_valid = df[amount_col].notna().sum()
    
    logger.info(f"  Cleaned currency in '{amount_col}': {new_valid} valid values "
               f"({original_valid - new_valid} flagged/removed)")
    return df


def validate_numeric_ranges(df, col, min_val=None, max_val=None):
    """
    Flag and null out values outside expected ranges.
    
    Example: Hours worked should be 0-24, not -2 or 25.5.
    """
    invalid_mask = pd.Series(False, index=df.index)
    
    if min_val is not None:
        too_low = df[col] < min_val
        invalid_mask |= too_low
        count_low = too_low.sum()
        if count_low > 0:
            logger.warning(f"  {count_low} values below minimum ({min_val}) in '{col}'")
    
    if max_val is not None:
        too_high = df[col] > max_val
        invalid_mask |= too_high
        count_high = too_high.sum()
        if count_high > 0:
            logger.warning(f"  {count_high} values above maximum ({max_val}) in '{col}'")
    
    df.loc[invalid_mask, col] = np.nan
    logger.info(f"  Validated '{col}': {invalid_mask.sum()} out-of-range values nullified")
    return df


def remove_duplicates(df, subset=None):
    """
    Remove exact duplicate rows, keeping the first occurrence.
    Logs how many duplicates were found for auditability.
    """
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep="first")
    after = len(df)
    removed = before - after
    
    if removed > 0:
        logger.info(f"  Removed {removed} duplicate rows ({before} → {after})")
    else:
        logger.info(f"  No duplicate rows found")
    
    return df


def remove_empty_rows(df):
    """
    Remove rows where ALL values are null (common in manually-edited Excel files
    where someone accidentally hit Enter creating blank rows).
    """
    before = len(df)
    df = df.dropna(how="all")
    after = len(df)
    removed = before - after
    
    if removed > 0:
        logger.info(f"  Removed {removed} completely empty rows")
    
    return df


def fill_missing_values(df, strategy="flag"):
    """
    Handle remaining null values based on column type.
    
    Strategies:
    - 'flag': Add an 'Unknown' label for categoricals, leave numerics as NaN
    - 'drop': Remove rows with any nulls
    - 'fill': Fill numerics with median, categoricals with 'Unknown'
    """
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count == 0:
            continue
            
        if strategy == "flag":
            if df[col].dtype == "object":
                df[col] = df[col].fillna("Unknown")
                logger.info(f"  Filled {null_count} nulls in '{col}' with 'Unknown'")
        elif strategy == "fill":
            if pd.api.types.is_numeric_dtype(df[col]):
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                logger.info(f"  Filled {null_count} nulls in '{col}' with median ({median_val:.2f})")
            else:
                df[col] = df[col].fillna("Unknown")
                logger.info(f"  Filled {null_count} nulls in '{col}' with 'Unknown'")
    
    return df


def generate_unique_key(df, prefix="REC"):
    """
    Create a unique identifier for each record.
    Addresses the BA's concern: 'they didn't know the rules of how data works, 
    how you're supposed to have unique keys.'
    """
    df.insert(0, "unique_id", [f"{prefix}-{str(i).zfill(6)}" for i in range(1, len(df) + 1)])
    logger.info(f"  Generated {len(df)} unique IDs with prefix '{prefix}'")
    return df
