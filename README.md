# Data Consolidation Pipeline

This project provides a robust data consolidation pipeline for processing vendor data from multiple sources. The pipeline includes field mapping, data cleaning, and duplicate handling capabilities.

## Key Features

- Automated field mapping with manual review options
- Data cleaning and standardisation
- Duplicate record detection and handling
- Comprehensive reporting system
- Historical mapping reference system

## File Structure

```
project/
├── raw_csvs/           # Original CSV files
├── cleaned_data/       # Cleaned individual CSV files
├── consolidated_data/  # Final consolidated outputs
│   ├── uncleaned_consolidated_data.csv
│   ├── cleaned_consolidated_data.csv
│   └── duplicate_records.csv
├── mapping/            # Field mapping configurations
│   ├── mapping_table.csv
│   └── historical_mapping_table.csv  # Historical reference mappings
└── scripts/
    ├── data_consolidation.py
    └── field_mapping_with_manual_review.py
```

## Process Flow

1. **Field Mapping (`field_mapping_with_manual_review.py`)**
   - Analyses raw CSV files
   - Generates draft mapping table
   - Supports manual review and updates
   - Maps fields to standardised names
   - References historical mappings for consistency

2. **Data Consolidation (`data_consolidation.py`)**
   - Loads and validates mapping table
   - Standardises data across sources
   - Applies cleaning rules
   - Handles duplicate records
   - Generates consolidated outputs

## Mapping Tables

### Current Mapping (`mapping_table.csv`)
- Active field mappings for current data processing
- Updated through manual review process
- Used directly by consolidation script

### Historical Mapping (`historical_mapping_table.csv`)
- Reference table of previous successful mappings
- Helps maintain consistency across different runs
- Serves as a lookup for common field mappings
- Useful for troubleshooting and auditing

## Output Files

The pipeline generates several output files in the `consolidated_data/` directory:

- `uncleaned_consolidated_data.csv`: Raw consolidated data before cleaning
- `cleaned_consolidated_data.csv`: Final cleaned and deduplicated data
- `duplicate_records.csv`: Records with duplicate vendor IDs

## Data Cleaning Rules

Current cleaning rules include:
- Removal of special characters from vendor IDs
- Whitespace removal from VAT numbers
- Duplicate vendor ID handling (first record kept)

## Usage

1. Place raw CSV files in `raw_csvs/` directory
2. Run field mapping script:
```bash
python field_mapping_with_manual_review.py
```
3. Review and update mapping table in `mapping/mapping_table.csv`
4. Run data consolidation script:
```bash
python data_consolidation.py
```

## Reports

The pipeline provides detailed reporting on:
- Duplicate vendor IDs
- Total records processed
- Cleaning operations performed
- Data standardisation results

## Dependencies

- pandas
- Python 3.x

********

Suggestions for improvement:


### Data Quality Improvements

1. Add metadata columns:
   - `last_updated`
   - `is_primary_address`
   - `is_primary_account`
   - `data_source`
   - `verification_status`

2. Create validation rules:
   - VAT number format check
   - IBAN validation
   - Address standardisation
   - Company name normalisation

3. Implement tracking:
   - Record merge history
   - Keep audit trail of changes
   - Document reason for duplicates