import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import logging

# Set up basic logging for warnings
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

def load_csv(file_path):
    """Loads a CSV file with error handling."""
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"File is empty: {file_path}")
        raise
    except Exception as e:
        logging.error(f"An error occurred loading {file_path}: {e}")
        raise

def clean_customers(df):
    """Cleans the customers dataset."""
    initial_rows = len(df)
    nulls_before = df.isnull().sum().to_dict()
    
    # Strip whitespace
    df['name'] = df['name'].str.strip()
    df['region'] = df['region'].str.strip()
    
    # Fill missing region
    df['region'] = df['region'].fillna('Unknown')
    
    # Email standardization and validation
    df['email'] = df['email'].str.lower()
    df['is_valid_email'] = df['email'].apply(
        lambda x: True if isinstance(x, str) and '@' in x and '.' in x else False
    )
    
    # Parse signup_date to datetime, forcing errors to NaT
    df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')
    
    # Log warning for unparseable dates
    unparseable_dates = df['signup_date'].isna().sum()
    if unparseable_dates > 0:
        logging.warning(f"Found {unparseable_dates} unparseable signup dates in customers.csv. Replaced with NaT.")
    
    # Sort by signup_date so we can keep the most recent when dropping duplicates
    df = df.sort_values('signup_date', ascending=False)
    df = df.drop_duplicates(subset=['customer_id'], keep='first')
    
    duplicates_removed = initial_rows - len(df)
    
    # Formatting date to YYYY-MM-DD string as requested
    df['signup_date'] = df['signup_date'].dt.strftime('%Y-%m-%d')
    
    nulls_after = df.isnull().sum().to_dict()
    
    return df, initial_rows, len(df), nulls_before, nulls_after, duplicates_removed

def parse_multi_format_date(val):
    """Custom parser for multiple date formats."""
    if pd.isna(val):
        return pd.NaT
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y'):
        try: 
            return pd.to_datetime(val, format=fmt)
        except (ValueError, TypeError): 
            pass
    return pd.NaT

def clean_orders(df):
    """Cleans the orders dataset."""
    initial_rows = len(df)
    nulls_before = df.isnull().sum().to_dict()
    
    # Drop rows where both customer_id and order_id are null
    df = df.dropna(subset=['customer_id', 'order_id'], how='all')
    rows_dropped = initial_rows - len(df)
    
    # Parse order date using custom parser
    df['order_date'] = df['order_date'].apply(parse_multi_format_date)
    
    # Create derived order_year_month column
    df['order_year_month'] = df['order_date'].dt.strftime('%Y-%m')
    
    # Impute missing amounts with median grouped by product
    df['amount'] = df['amount'].fillna(df.groupby('product')['amount'].transform('median'))
    
    # Normalize status vocabulary
    status_map = {
        'done': 'completed',
        'canceled': 'cancelled',
        'successful': 'completed',
        'pending': 'pending',
        'completed': 'completed',
        'refunded': 'refunded',
        'cancelled': 'cancelled'
    }
    # Lowercase first, then map variants, keeping unmapped items as is (or map to unknown if preferred)
    df['status'] = df['status'].str.lower().str.strip()
    df['status'] = df['status'].map(status_map).fillna(df['status'])
    
    # Enforce vocabulary list filter (optional safety check)
    valid_statuses = ['completed', 'pending', 'cancelled', 'refunded']
    df['status'] = df['status'].apply(lambda x: x if x in valid_statuses else 'unknown')

    nulls_after = df.isnull().sum().to_dict()
    
    return df, initial_rows, len(df), nulls_before, nulls_after, rows_dropped

def print_report(name, initial, final, before_nulls, after_nulls, dropped):
    """Prints a formatted cleaning report."""
    print(f"--- {name.upper()} CLEANING REPORT ---")
    print(f"Rows Before: {initial} | Rows After: {final} | Rows Dropped: {dropped}")
    print("Null Counts Before:")
    for col, count in before_nulls.items():
        if count > 0: print(f"  - {col}: {count}")
    print("Null Counts After:")
    for col, count in after_nulls.items():
        if count > 0: print(f"  - {col}: {count}")
    print("-" * 40 + "\n")

def main():
    parser = argparse.ArgumentParser(description='Clean raw data files.')
    parser.add_argument('--input_dir', type=str, default='data/raw', help='Directory containing raw CSVs')
    parser.add_argument('--output_dir', type=str, default='data/processed', help='Directory for cleaned CSVs')
    args = parser.parse_args()

    # Setup paths relative to the script location
    base_path = Path(__file__).parent
    input_path = base_path / args.input_dir
    output_path = base_path / args.output_dir
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)

    print("Starting data cleaning pipeline...\n")

    # 1. Process Customers
    customers_file = input_path / 'customers.csv'
    df_cust = load_csv(customers_file)
    df_cust_clean, c_init, c_final, c_nulls_b, c_nulls_a, c_dupes = clean_customers(df_cust)
    df_cust_clean.to_csv(output_path / 'customers_clean.csv', index=False)
    print_report("Customers", c_init, c_final, c_nulls_b, c_nulls_a, c_dupes)

    # 2. Process Orders
    orders_file = input_path / 'orders.csv'
    df_ord = load_csv(orders_file)
    df_ord_clean, o_init, o_final, o_nulls_b, o_nulls_a, o_dropped = clean_orders(df_ord)
    df_ord_clean.to_csv(output_path / 'orders_clean.csv', index=False)
    print_report("Orders", o_init, o_final, o_nulls_b, o_nulls_a, o_dropped)
    
    print(f"Cleaning complete! Files saved to {output_path}")

if __name__ == "__main__":
    main()