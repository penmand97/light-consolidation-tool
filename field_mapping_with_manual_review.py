import pandas as pd
import os

# Data Cleaning Functions
def find_first_nonempty_row(file_path):
    """Find the index of the first non-empty row in the CSV."""
    with open(file_path, 'r') as file:
        for index, line in enumerate(file):
            if line.strip() and not all(cell.strip() == '' for cell in line.split(',')):
                return index
    return 0

def clean_csv(df):
    """Clean a CSV DataFrame."""
    df = df.dropna(how='all')  # Remove completely empty rows
    df = df.fillna(method='ffill')  # Forward-fill merged cells
    df = df.drop_duplicates()  # Remove duplicate rows
    return df

# Field Mapping and Consolidation Functions
def analyse_cleaned_data(cleaned_dataframes):
    """Analyse columns and sample data from cleaned files."""
    column_analysis = {}
    all_columns = set()

    for file_name, df in cleaned_dataframes.items():
        all_columns.update(df.columns)
        column_analysis[file_name] = {
            'columns': list(df.columns),
            'sample_data': df.head(1).to_dict('records')[0] if not df.empty else {}
        }

    return column_analysis, sorted(all_columns)

def create_mapping_table(column_analysis, standard_fields):
    """Generate a draft mapping table with potential mappings and unmapped fields."""
    mapping_data = []
    for file_name, details in column_analysis.items():
        for column in details['columns']:
            mapped_field = next(
                (key for key, values in standard_fields.items() if column in values), None
            )
            mapping_data.append({
                'Source': file_name,
                'Source Field': column,
                'Standard Field': mapped_field if mapped_field else 'Unmapped',
                'Sample Data': details['sample_data'].get(column, None),
            })
    return pd.DataFrame(mapping_data)

def save_mapping_table(mapping_table, output_path):
    """Save the mapping table as a CSV file for manual review."""
    mapping_table.to_csv(output_path, index=False)
    print(f"Mapping table saved to: {output_path}")

def load_mapping_table(file_path):
    """Load the manually updated mapping table."""
    return pd.read_csv(file_path)

def apply_mapping(cleaned_dataframes, mapping_table):
    """Apply the updated mapping table to consolidate data."""
    consolidated_data = pd.DataFrame()

    for source_file, df in cleaned_dataframes.items():
        source_mapping = mapping_table[mapping_table['Source'] == source_file]
        for _, row in source_mapping.iterrows():
            if row['Standard Field'] != 'Unmapped':
                df = df.rename(columns={row['Source Field']: row['Standard Field']})
        consolidated_data = pd.concat([consolidated_data, df], ignore_index=True)

    # Create consolidated folder and save the data
    os.makedirs('./consolidated_data', exist_ok=True)
    output_path = './consolidated_data/consolidated_data.csv'
    consolidated_data.to_csv(output_path, index=False)
    print(f"Consolidated data saved to: {output_path}")

    return consolidated_data

if __name__ == "__main__":
    # Define paths and standard fields
    raw_folder = './raw_csvs'  # Folder containing raw CSVs
    cleaned_folder = './cleaned_data'  # Folder to save cleaned data
    mapping_folder = './mapping'  # New folder for mapping files
    mapping_output_path = './mapping/mapping_table.csv'  # Updated mapping CSV path
    consolidated_output_path = './cleaned_data/consolidated_data.csv'  # Final output

    # Create necessary directories
    os.makedirs(cleaned_folder, exist_ok=True)
    os.makedirs(mapping_folder, exist_ok=True)  # Create mapping folder

    # Standard field mappings
    standard_fields = {
        'vendor_id': ['Vendor ID', 'Vendor Number', 'Vendor identifier', 'Vendor ID Number'],
        'vendor_name': ['Vendor name', 'Name', 'Vendor Name', 'Description'],
        'address': ['Address', 'Company Address'],
        'postal_code': ['ZIP/postcode', 'ZIP', 'Postcode'],
        'city': ['City'],
        'country': ['Country'],
        'email': ['Email', 'Email for Contact'],
        'vat_number': ['VAT Code', 'VAT-No'],
        'currency': ['Currency', 'Currency code'],
        'iban': ['IBAN'],
        'bic': ['BIC'],
        'bank_name': ['Bank Name', 'Bank name'],
        'bank_country': ['Bank country', 'Bank Country'],
        'company_entities': ['Company entities'],
        'group': ['Vendor Group', 'Groups', 'Group'],
        'payment_terms': ['Payment terms'],
        'bank_account_number': ['Bank Account Number'],
        'bank_code': ['Bank Code'],
        'norwegian_bankgiro_number': ['Norwegian Bankgiro Number'],
        'owner': ['Owner']
    }

    # Step 1: Clean raw data
    cleaned_dataframes = {}

    for file in os.listdir(raw_folder):
        if file.endswith('.csv'):
            raw_path = os.path.join(raw_folder, file)
            header_row = find_first_nonempty_row(raw_path)
            df = pd.read_csv(raw_path, header=header_row, dtype=str)
            cleaned_df = clean_csv(df)
            cleaned_dataframes[file] = cleaned_df
            cleaned_df.to_csv(os.path.join(cleaned_folder, file), index=False)

    # Step 2: Analyse cleaned data
    column_analysis, all_columns = analyse_cleaned_data(cleaned_dataframes)

    # Step 3: Generate mapping table
    mapping_table = create_mapping_table(column_analysis, standard_fields)
    save_mapping_table(mapping_table, mapping_output_path)

    print("Please review and update the mapping table, then re-run this script to consolidate the data.")
