import pandas as pd
import numpy as np
import argparse
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def load_csv(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw_dir', type=str, default='data/raw')
    parser.add_argument('--input_dir', type=str, default='data/processed')
    parser.add_argument('--output_dir', type=str, default='data/processed')
    args = parser.parse_args()

    base_path = Path(__file__).parent
    raw_path = base_path / args.raw_dir
    input_path = base_path / args.input_dir
    output_path = base_path / args.output_dir
    output_path.mkdir(parents=True, exist_ok=True)

    logging.info("--- LOADING DATA ---")
    customers = load_csv(input_path / 'customers_clean.csv')
    orders = load_csv(input_path / 'orders_clean.csv')
    products = load_csv(raw_path / 'products.csv')

    logging.info("--- MERGING DATA ---")
    orders_with_customers = pd.merge(orders, customers, on='customer_id', how='left')
    full_data = pd.merge(orders_with_customers, products, left_on='product', right_on='product_name', how='left')

    logging.info("--- PERFORMING ANALYSIS ---")
    full_data['order_date'] = pd.to_datetime(full_data['order_date'], errors='coerce')
    full_data['amount'] = pd.to_numeric(full_data['amount'], errors='coerce')
    completed_orders = full_data[full_data['status'] == 'completed'].copy()

    # 1. Revenue
    monthly_revenue = completed_orders.groupby('order_year_month', as_index=False)['amount'].sum()
    monthly_revenue.rename(columns={'amount': 'total_revenue'}, inplace=True)
    monthly_revenue.to_csv(output_path / 'monthly_revenue.csv', index=False)
    logging.info("Saved monthly_revenue.csv")

    # 2. Categories
    category_performance = completed_orders.groupby('category').agg(
        total_revenue=('amount', 'sum'),
        average_order_value=('amount', 'mean'),
        number_of_orders=('order_id', 'nunique')
    ).reset_index()
    category_performance.to_csv(output_path / 'category_performance.csv', index=False)
    logging.info("Saved category_performance.csv")

    # 3. Regions
    regional_analysis = completed_orders.groupby('region').agg(
        number_of_customers=('customer_id', 'nunique'),
        number_of_orders=('order_id', 'nunique'),
        total_revenue=('amount', 'sum')
    ).reset_index()
    regional_analysis.to_csv(output_path / 'regional_analysis.csv', index=False)
    logging.info("Saved regional_analysis.csv")

    # 4. Top Customers & Churn
    top_spenders = completed_orders.groupby(['customer_id', 'name', 'region'])['amount'].sum().reset_index()
    top_spenders.rename(columns={'amount': 'total_spend'}, inplace=True)
    top_10 = top_spenders.nlargest(10, 'total_spend')
    
    latest_date = full_data['order_date'].max()
    ninety_days_ago = latest_date - pd.Timedelta(days=90)
    
    last_order_dates = completed_orders.groupby('customer_id')['order_date'].max().reset_index()
    last_order_dates.rename(columns={'order_date': 'last_completed_order_date'}, inplace=True)
    
    top_10 = pd.merge(top_10, last_order_dates, on='customer_id', how='left')
    top_10['churned'] = (top_10['last_completed_order_date'] < ninety_days_ago) | (top_10['last_completed_order_date'].isna())
    
    top_customers_final = top_10[['name', 'region', 'total_spend', 'churned']]
    top_customers_final.to_csv(output_path / 'top_customers.csv', index=False)
    logging.info("Saved top_customers.csv")
    logging.info("Analysis complete!")

if __name__ == "__main__":
    main()