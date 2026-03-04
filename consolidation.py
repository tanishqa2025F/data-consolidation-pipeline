"""
consolidation.py
Merges data from multiple sources into a single unified dataset.

Core challenge this solves: Different systems use different column names 
for the same data (e.g., 'Issue Date' vs 'Report Date' vs 'DATE'), different
status codes, and different formatting conventions.

Uses a JSON config file (config/field_mappings.json) to map source-specific
column names to a standardized schema — making it easy to add new data 
sources without changing code.
"""

import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)


def load_field_mappings(config_path):
    """Load column name mappings from configuration file."""
    with open(config_path, "r") as f:
        config = json.load(f)
    logger.info(f"Loaded field mappings for {len(config['column_mappings'])} data sources")
    return config


def rename_columns(df, source_name, mappings):
    """
    Rename columns from source-specific names to standardized names.
    
    Example:
        System A: 'Issue Date' → 'date'
        System B: 'Report Date' → 'date'  
        Manual:   'DATE' → 'date'
    
    All three now have the same column name, ready for merging.
    """
    column_map = mappings.get("column_mappings", {}).get(source_name, {})
    
    if not column_map:
        logger.warning(f"No column mappings found for source '{source_name}'")
        return df
    
    # Only rename columns that exist in the dataframe
    valid_renames = {k: v for k, v in column_map.items() if k in df.columns}
    unmapped = [c for c in df.columns if c not in column_map]
    
    df = df.rename(columns=valid_renames)
    
    logger.info(f"  Renamed {len(valid_renames)} columns for '{source_name}'")
    if unmapped:
        logger.info(f"  Unmapped columns (kept as-is): {unmapped}")
    
    return df


def standardize_categorical(df, col, mapping):
    """
    Map inconsistent categorical values to standard labels.
    
    Example:
        'Complete', 'Completed', 'DONE', 'Done' → all become 'Completed'
        'WIP', 'In Progress', 'In-Progress' → all become 'In Progress'
    """
    if col not in df.columns:
        return df
    
    before_unique = df[col].nunique()
    df[col] = df[col].map(mapping).fillna(df[col])
    after_unique = df[col].nunique()
    
    if before_unique != after_unique:
        logger.info(f"  Standardized '{col}': {before_unique} → {after_unique} unique values")
    
    return df


def add_source_tracking(df, source_name):
    """
    Add metadata columns to track which system each record came from.
    Essential for debugging and auditing the consolidated dataset.
    """
    df["data_source"] = source_name
    return df


def consolidate_sources(dataframes_dict, config_path):
    """
    Main consolidation function: takes multiple dataframes from different sources,
    standardizes their schemas, and merges into one unified dataset.
    
    Parameters:
        dataframes_dict: dict of {source_name: dataframe}
        config_path: path to field_mappings.json
    
    Returns:
        Consolidated pandas DataFrame with standardized columns
    """
    mappings = load_field_mappings(config_path)
    standardized_dfs = []
    
    logger.info("=" * 60)
    logger.info("CONSOLIDATION: Standardizing schemas across data sources")
    logger.info("=" * 60)
    
    for source_name, df in dataframes_dict.items():
        logger.info(f"\nProcessing: {source_name} ({len(df)} rows, {len(df.columns)} columns)")
        
        # Step 1: Rename columns to standard names
        df = rename_columns(df, source_name, mappings)
        
        # Step 2: Standardize status values
        if "order_status" in df.columns:
            df = standardize_categorical(
                df, "order_status", mappings.get("status_standardization", {})
            )
        
        # Step 3: Standardize service type values
        if "service_type" in df.columns:
            df = standardize_categorical(
                df, "service_type", mappings.get("service_type_standardization", {})
            )
        
        # Step 4: Add source tracking
        df = add_source_tracking(df, source_name)
        
        standardized_dfs.append(df)
        logger.info(f"  Final columns: {list(df.columns)}")
    
    # Merge all sources (outer join to keep all records)
    consolidated = pd.concat(standardized_dfs, ignore_index=True, sort=False)
    
    # Reorder columns: put common fields first
    priority_cols = ["unique_id", "date", "customer_name", "technician_name", 
                     "service_type", "revenue", "order_status", "service_location",
                     "hours_worked", "overtime_hours", "hourly_rate", "data_source"]
    
    existing_priority = [c for c in priority_cols if c in consolidated.columns]
    remaining = [c for c in consolidated.columns if c not in priority_cols]
    consolidated = consolidated[existing_priority + remaining]
    
    logger.info(f"\nConsolidation complete: {len(consolidated)} total rows, "
               f"{len(consolidated.columns)} columns")
    logger.info(f"Sources merged: {list(dataframes_dict.keys())}")
    
    return consolidated


def validate_consolidated_data(df):
    """
    Run validation checks on the final consolidated dataset.
    Returns a dict of quality metrics for the report.
    """
    metrics = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "null_percentage": (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
        "duplicate_rows": df.duplicated().sum(),
        "sources": df["data_source"].value_counts().to_dict() if "data_source" in df.columns else {},
        "date_range": {
            "min": str(df["date"].min()) if "date" in df.columns else "N/A",
            "max": str(df["date"].max()) if "date" in df.columns else "N/A",
        }
    }
    
    logger.info("\n--- Data Quality Report ---")
    logger.info(f"Total records: {metrics['total_rows']}")
    logger.info(f"Duplicate rows: {metrics['duplicate_rows']}")
    logger.info(f"Date range: {metrics['date_range']['min']} to {metrics['date_range']['max']}")
    
    high_null_cols = {k: v for k, v in metrics["null_percentage"].items() if v > 5}
    if high_null_cols:
        logger.warning(f"Columns with >5% nulls: {high_null_cols}")
    
    return metrics
