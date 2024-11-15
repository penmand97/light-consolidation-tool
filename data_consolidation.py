import pandas as pd
import os

def load_mapping_table(mapping_file):
    """Load the mapping table."""
    return pd.read_csv(mapping_file)

def load_data(files):
    """Load all datasets into a dictionary."""
    data_dict = {}
    for file_name, file_path in files.items():
        data_dict[file_name] = pd.read_csv(file_path)
    return data_dict

def standardise_data(data, source_name, mapping_table):
    """Standardise a dataset based on the mapping table with vendor ID validation."""
    # Before standardisation
    if 'vendor_id' in data.columns:
        print(f"\nVendor IDs before standardisation in {source_name}:")
        print(data['vendor_id'].head())
    
    mapping = mapping_table[mapping_table['Source'] == source_name]
    field_map = dict(zip(mapping['Source Field'], mapping['Standard Field']))
    
    # Ensure vendor_id mapping exists
    vendor_id_mappings = mapping[mapping['Standard Field'] == 'vendor_id']
    if len(vendor_id_mappings) == 0:
        print(f"\nWARNING: No vendor_id mapping found for {source_name}")
        print("Current mappings:", field_map)
    
    standardised = data.rename(columns=field_map)
    
    # After standardisation
    if 'vendor_id' in standardised.columns:
        print(f"\nVendor IDs after standardisation in {source_name}:")
        print(standardised['vendor_id'].head())
    else:
        print(f"\nERROR: vendor_id column missing after standardisation in {source_name}")
    
    return standardised

def ensure_unique_columns(df):
    """Ensure unique column names."""
    df.columns = pd.Index([f"{col}_{i}" if list(df.columns).count(col) > 1 else col
                           for i, col in enumerate(df.columns)])
    return df

def consolidate_data(data_dict, mapping_table):
    """Consolidate all datasets."""
    standardised_data = []
    for source_name, data in data_dict.items():
        standardised = standardise_data(data, source_name, mapping_table)
        standardised = ensure_unique_columns(standardised)
        standardised_data.append(standardised)
    consolidated = pd.concat(standardised_data, ignore_index=True)
    return consolidated

def clean_columns(df, column_cleaning_rules):
    """Clean specific columns with validation."""
    if 'vendor_id' in df.columns:
        print("\nVendor IDs before cleaning:")
        print(df['vendor_id'].head())
        
        # Count blank vendor IDs
        blank_vendors = df['vendor_id'].isna().sum()
        if blank_vendors > 0:
            print(f"\nWARNING: Found {blank_vendors} blank vendor IDs")
    
    for column, cleaning_function in column_cleaning_rules.items():
        if column in df.columns:
            df[column] = df[column].apply(cleaning_function)
    
    if 'vendor_id' in df.columns:
        print("\nVendor IDs after cleaning:")
        print(df['vendor_id'].head())
    
    return df

# Example column cleaning rules
def remove_special_characters(value):
    if pd.notnull(value):
        return ''.join(e for e in str(value) if e.isalnum())
    return value

def remove_whitespace(value):
    if pd.notnull(value):
        return ''.join(value.split())
    return value

# Main process
def main(mapping_file, data_files):
    mapping_table = load_mapping_table(mapping_file)
    print("\nMapping table contents:")
    print(mapping_table.head())
    
    data_dict = load_data(data_files)
    
    # Print column names for each file
    for file_name, df in data_dict.items():
        print(f"\nColumns in {file_name}:")
        print(df.columns.tolist())
    
    # Consolidate all data
    consolidated = consolidate_data(data_dict, mapping_table)
    
    # Save uncleaned version
    os.makedirs('./consolidated_data', exist_ok=True)
    consolidated.to_csv('./consolidated_data/uncleaned_consolidated_data.csv', index=False)
    print("\nSaved uncleaned consolidated data to './consolidated_data/uncleaned_consolidated_data.csv'")
    
    # Clean specific columns
    cleaning_rules = {
        'vendor_id': remove_special_characters,
        'vat_number': remove_whitespace
    }
    consolidated_cleaned = clean_columns(consolidated, cleaning_rules)
    
    # Drop duplicates if applicable
    if 'vendor_id' in consolidated_cleaned.columns:
        consolidated_cleaned = consolidated_cleaned.drop_duplicates(subset=['vendor_id'], keep='first')
    
    return consolidated_cleaned

# Paths to the mapping table and data files
mapping_file_path = './mapping/mapping_table.csv'
data_files_paths = {
    "BE.csv": './cleaned_data/BE.csv',
    "CH.csv": './cleaned_data/CH.csv',
    "CZ.csv": './cleaned_data/CZ.csv',
    "ES.csv": './cleaned_data/ES.csv',
    "NO.csv": './cleaned_data/NO.csv'
}

# Execute the script
if __name__ == "__main__":
    final_data = main(mapping_file_path, data_files_paths)
    
    # Save cleaned version with new naming
    final_data.to_csv('./consolidated_data/cleaned_consolidated_data.csv', index=False)
    print("\nSaved cleaned consolidated data to './consolidated_data/cleaned_consolidated_data.csv'")
    
    # Create duplicates report
    try:
        uncleaned = pd.read_csv('./consolidated_data/uncleaned_consolidated_data.csv')
        cleaned = pd.read_csv('./consolidated_data/cleaned_consolidated_data.csv')
        
        # Find duplicate records based on vendor_id
        if 'vendor_id' in uncleaned.columns:
            # Get all duplicated vendor IDs
            duplicate_ids = uncleaned[uncleaned.duplicated(subset=['vendor_id'], keep=False)]['vendor_id'].unique()
            
            # Get full records of duplicates
            duplicate_records = uncleaned[uncleaned['vendor_id'].isin(duplicate_ids)].sort_values('vendor_id')
            duplicate_records.to_csv('./consolidated_data/duplicate_records.csv', index=False)
            print("\nSaved duplicate records to './consolidated_data/duplicate_records.csv'")
            
            # Print summary
            print("\nDuplicates Summary:")
            print(f"Total rows in uncleaned: {len(uncleaned)}")
            print(f"Unique vendor IDs: {uncleaned['vendor_id'].nunique()}")
            print(f"Number of duplicate vendor IDs: {len(duplicate_ids)}")
            print(f"Total duplicate rows: {len(duplicate_records)}")
            
    except Exception as e:
        print(f"Error creating duplicates report: {str(e)}")