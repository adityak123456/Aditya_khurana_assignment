import pandas as pd
import numpy as np
from pathlib import Path

def generate_dummy_data():
    # Setup the data/raw directory
    base_path = Path(__file__).parent
    raw_dir = base_path / 'data' / 'raw'
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating dummy data in: {raw_dir}")

    # 1. Generate Products Data (product catalog; some products won't be in orders)
    products_data = {
        'product_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Accessories'],
        'unit_price': [1200.00, 25.50, 75.00, 300.00, 60.00]
    }
    df_products = pd.DataFrame(products_data)
    df_products.to_csv(raw_dir / 'products.csv', index=False)
    print("Created products.csv")

    # 2. Generate Customers Data (includes duplicates, missing emails, malformed emails, bad dates, missing regions)
    customers_data = {
        'customer_id': ['C1', 'C2', 'C3', 'C1', 'C4', 'C5'],
        'name': [' Alice ', 'Bob', 'Charlie', 'Alice', ' Diana ', 'Eve'], # Leading/trailing whitespace
        'email': ['alice@example.com', 'bob_at_example.com', None, 'alice@example.com', 'diana@domain', 'eve@example.com'], # Malformed and missing
        'region': ['North', 'South', None, 'North', ' East ', 'West'], # Missing and whitespace
        'signup_date': ['2023-01-15', '2023-02-20', 'not-a-date', '2023-05-10', '2023-03-12', '2023/13/45'] # Unparseable dates and duplicate C1 with newer date
    }
    df_customers = pd.DataFrame(customers_data)
    df_customers.to_csv(raw_dir / 'customers.csv', index=False)
    print("Created customers.csv")

    # 3. Generate Orders Data (mixed date formats, null amounts, unrecoverable rows, status variants)
    orders_data = {
        'order_id': ['O101', 'O102', 'O103', 'O104', None, 'O106'],
        'customer_id': ['C1', 'C2', 'C3', 'C4', None, 'C5'], # Null row for unrecoverable check
        'product': ['Laptop', 'Mouse', 'Mouse', 'Monitor', 'Keyboard', 'Laptop'],
        'amount': [1200.00, None, 25.50, None, 75.00, 1150.00], # Missing amounts
        'order_date': ['2023-06-01', '15/06/2023', '06-20-2023', '2023-07-01', '2023-07-05', '07/10/2023'], # Mixed formats
        'status': ['done', 'pending', 'canceled', 'completed', 'refunded', 'SUCCESSFUL'] # Obvious variants
    }
    df_orders = pd.DataFrame(orders_data)
    df_orders.to_csv(raw_dir / 'orders.csv', index=False)
    print("Created orders.csv")

    print("\nDone! You can now run your clean_data.py script.")

if __name__ == "__main__":
    generate_dummy_data()