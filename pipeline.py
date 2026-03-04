"""
pipeline.py
Main orchestrator for the data consolidation pipeline.

Takes messy Excel files from multiple business systems, cleans and standardizes them,
merges into a single consolidated dataset, and auto-generates a summary report.

Usage:
    python src/pipeline.py                    # Run full pipeline
    python src/pipeline.py --generate-data    # Generate sample data first, then run

This pipeline demonstrates:
    - Data ingestion from multiple Excel sources
    - Schema standardization (different column names → unified schema)
    - Data cleaning (dates, names, currencies, duplicates, nulls)
    - Automated report generation (HTML with embedded charts)
    - Full logging for auditability
"""

import pandas as pd
import os
import sys
import logging
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.cleaning import (
    standardize_dates, clean_names, clean_currency,
    validate_numeric_ranges, remove_duplicates, remove_empty_rows,
    fill_missing_values, generate_unique_key
)
from src.consolidation import consolidate_sources, validate_consolidated_data
from src.reporting import generate_html_report


def setup_logging():
    """Configure logging to both file and console."""
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def load_raw_data(raw_dir):
    """Load all Excel files from the raw data directory."""
    dataframes = {}
    
    for filename in sorted(os.listdir(raw_dir)):
        if filename.endswith((".xlsx", ".xls")):
            filepath = os.path.join(raw_dir, filename)
            source_name = filename.replace(".xlsx", "").replace(".xls", "")
            
            df = pd.read_excel(filepath)
            dataframes[source_name] = df
            logging.info(f"Loaded '{filename}': {len(df)} rows × {len(df.columns)} columns")
    
    if not dataframes:
        raise FileNotFoundError(f"No Excel files found in {raw_dir}")
    
    return dataframes


def clean_source(df, source_name):
    """
    Apply source-specific cleaning operations.
    Each source has different data quality issues that need targeted fixes.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"\n{'='*60}")
    logger.info(f"CLEANING: {source_name}")
    logger.info(f"{'='*60}")
    logger.info(f"Starting shape: {df.shape}")
    
    # Universal cleaning steps
    df = remove_empty_rows(df)
    df = remove_duplicates(df)
    
    # Source-specific cleaning
    if source_name == "service_orders_system_a":
        df = standardize_dates(df, "Issue Date")
        df = clean_names(df, "Technician Assigned")
        df = clean_currency(df, "Total Charged")
        
    elif source_name == "technician_hours_system_b":
        df = standardize_dates(df, "Report Date")
        df = clean_names(df, "Tech Name")
        df = validate_numeric_ranges(df, "Hrs Worked", min_val=0, max_val=24)
        df = validate_numeric_ranges(df, "OT Hours", min_val=0, max_val=16)
        
    elif source_name == "revenue_tracking_manual":
        df = standardize_dates(df, "DATE")
        df = clean_names(df, "TECH")
        df = clean_currency(df, "AMT")
    
    logger.info(f"Final shape after cleaning: {df.shape}")
    return df


def run_pipeline():
    """
    Execute the full data consolidation pipeline.
    
    Steps:
        1. Load raw data from multiple Excel sources
        2. Clean each source independently (source-specific rules)
        3. Consolidate into unified schema (standardize column names)
        4. Apply post-merge cleaning and validation
        5. Generate unique keys for every record
        6. Export consolidated data to CSV
        7. Auto-generate HTML summary report
    """
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("DATA CONSOLIDATION PIPELINE — START")
    logger.info(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # Paths
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    processed_dir = os.path.join(PROJECT_ROOT, "data", "processed")
    reports_dir = os.path.join(PROJECT_ROOT, "reports")
    config_path = os.path.join(PROJECT_ROOT, "config", "field_mappings.json")
    
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    
    # --- Step 1: Generate sample data if needed ---
    if "--generate-data" in sys.argv or not os.path.exists(raw_dir) or not os.listdir(raw_dir):
        logger.info("\nGenerating sample data...")
        from src.generate_sample_data import main as generate_data
        generate_data()
    
    # --- Step 2: Load raw data ---
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: Loading raw data sources")
    logger.info("=" * 60)
    raw_data = load_raw_data(raw_dir)
    logger.info(f"Loaded {len(raw_data)} data sources")
    
    # --- Step 3: Clean each source ---
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Cleaning individual sources")
    logger.info("=" * 60)
    cleaned_data = {}
    for source_name, df in raw_data.items():
        cleaned_data[source_name] = clean_source(df.copy(), source_name)
    
    # --- Step 4: Consolidate ---
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Consolidating data sources")
    logger.info("=" * 60)
    consolidated = consolidate_sources(cleaned_data, config_path)
    
    # --- Step 5: Post-merge cleaning ---
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Post-merge cleaning & validation")
    logger.info("=" * 60)
    consolidated = standardize_dates(consolidated, "date")
    consolidated = fill_missing_values(consolidated, strategy="flag")
    consolidated = generate_unique_key(consolidated, prefix="EMC")
    
    # --- Step 6: Validate ---
    metrics = validate_consolidated_data(consolidated)
    
    # --- Step 7: Export ---
    output_csv = os.path.join(processed_dir, "consolidated_data.csv")
    consolidated.to_csv(output_csv, index=False)
    logger.info(f"\nConsolidated data saved to: {output_csv}")
    
    # --- Step 8: Generate report ---
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: Generating automated report")
    logger.info("=" * 60)
    report_path = os.path.join(reports_dir, "summary_report.html")
    generate_html_report(consolidated, metrics, report_path)
    
    # --- Summary ---
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Input:  {len(raw_data)} Excel files from {raw_dir}")
    logger.info(f"Output: {len(consolidated)} consolidated records → {output_csv}")
    logger.info(f"Report: {report_path}")
    logger.info(f"Logs:   {os.path.join(PROJECT_ROOT, 'logs')}/")
    
    return consolidated, metrics


if __name__ == "__main__":
    run_pipeline()
