# Data Consolidation & Automated Reporting Pipeline

Python pipeline that takes messy data from multiple disconnected business 
systems, standardizes schemas, cleans data quality issues, and auto-generates 
a summary report with visualizations.

**Built to solve a real problem:** Companies often have data scattered across 
different systems with inconsistent naming conventions, no unique keys, and 
no single source of truth. This pipeline consolidates everything into one 
clean, validated dataset automatically.

![Report Preview](screenshots/report_preview.png)

## What It Does

1. **Ingests** Excel files from 3 different simulated business systems
2. **Standardizes** column names using a configurable JSON mapping (e.g., "Issue Date", "Report Date", and "DATE" all become "date")
3. **Cleans** data quality issues: inconsistent date formats, name variations, invalid values, currency formatting, duplicates, empty rows
4. **Consolidates** into a single unified dataset with proper unique keys
5. **Validates** the output with quality metrics
6. **Generates** an HTML report with KPI cards, revenue charts, monthly trends, and technician performance
7. **Logs** every operation for full auditability

## Quick Start
```bash
git clone https://github.com/YOUR_USERNAME/data-consolidation-pipeline.git
cd data-consolidation-pipeline
pip install -r requirements.txt
python src/pipeline.py --generate-data
```

Then open `reports/summary_report.html` in your browser.

## Output

- `data/processed/consolidated_data.csv` — clean unified dataset
- `reports/summary_report.html` — auto-generated dashboard report
- `logs/` — full audit trail of every operation

## Data Quality Issues Handled

| Issue | Example | Solution |
|-------|---------|----------|
| Inconsistent column names | "Issue Date" vs "Report Date" vs "DATE" | JSON-based field mapping |
| Mixed date formats | "01/15/2025" vs "2025-01-15" vs "01-15-25" | Automatic multi-format parsing |
| Name variations | "mike johnson" vs "Mike J. Johnson" | Strip, normalize case, remove initials |
| Currency formatting | "$1,234.56" vs "1234.56" | Regex cleaning + type conversion |
| Invalid values | Negative hours, 25-hour workdays | Range validation with configurable bounds |
| Duplicate records | Exact row duplicates | Deduplication keeping first occurrence |
| Empty rows | Blank Excel rows from manual entry | Drop all-null rows |
| No unique keys | Records with no identifier | Auto-generated unique IDs |

## Tech Stack

- **Python 3.10+**
- **pandas** — data manipulation and cleaning
- **openpyxl** — Excel file I/O
- **matplotlib** — chart generation
- **JSON config** — no hardcoded column names

## Project Structure
```
data-consolidation-pipeline/
├── config/
│   └── field_mappings.json        # Column name standardization rules
├── src/
│   ├── pipeline.py                # Main orchestrator
│   ├── generate_sample_data.py    # Creates realistic messy test data
│   ├── cleaning.py                # Data cleaning functions
│   ├── consolidation.py           # Schema standardization & merging
│   └── reporting.py               # Auto HTML report generation
├── data/
│   ├── raw/                       # Input: messy Excel files
│   └── processed/                 # Output: clean consolidated CSV
├── reports/                       # Auto-generated HTML reports
└── logs/                          # Pipeline execution logs
```

## Configuration

Adding a new data source requires zero code changes — just update `config/field_mappings.json`:
```json
{
    "column_mappings": {
        "new_system_name": {
            "Their Column Name": "your_standard_name"
        }
    }
}
```

## Context

This project was inspired by real data consolidation challenges encountered 
during my internship, where I worked with messy financial datasets across 
multiple systems. The pattern of different column names for the same data, 
inconsistent formatting, and manual Excel-based processes is extremely common 
in organizations building out their data capabilities.
