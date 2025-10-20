"""
SAS-equivalent simulation using Python/pandas
This script replicates the exact logic of the SAS program to generate sample outputs
for comparison purposes.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(12345)

print("Creating customers dataset...")
customers_data = []
for customer_id in range(1000, 1020):  # 20 customers
    name = f'Cust_{customer_id:4d}'
    pick = np.random.randint(1, 5)
    region_map = {1: 'North', 2: 'South', 3: 'East', 4: 'West'}
    region = region_map[pick]
    signup_date = datetime.today() - timedelta(days=np.random.randint(0, 365*3))
    customers_data.append({
        'CustomerID': customer_id,
        'Name': name,
        'Region': region,
        'SignupDate': signup_date
    })

customers = pd.DataFrame(customers_data)
print(f"Created {len(customers)} customers")

print("Creating transactions dataset...")
np.random.seed(98765)
transactions_data = []
transaction_id = 0

for i in range(600):  # 600 transactions
    transaction_id += 1
    customer_id = 1000 + int(np.ceil(np.random.uniform() * 20)) - 1
    tran_date = datetime.today() - timedelta(days=np.random.randint(0, 365*2))
    
    amount = round(np.random.normal(200, 600), 2)
    
    if np.random.uniform() < 0.01:
        amount = amount * 10
    
    pm = np.random.randint(1, 5)
    payment_method_map = {1: 'Card', 2: 'ACH', 3: 'Check', 4: 'Cash'}
    payment_method = payment_method_map[pm]
    
    st = np.random.uniform()
    if st < 0.02:
        status = ''
    elif st < 0.9:
        status = 'Completed'
    else:
        status = 'Refunded'
    
    transactions_data.append({
        'TransactionID': transaction_id,
        'CustomerID': customer_id,
        'TranDate': tran_date,
        'Amount': amount,
        'PaymentMethod': payment_method,
        'Status': status
    })

transactions = pd.DataFrame(transactions_data)
print(f"Created {len(transactions)} transactions")

print("Cleaning transactions...")
tx_clean = transactions.copy()

tx_clean['Status_clean'] = tx_clean['Status'].apply(
    lambda x: 'Unknown' if pd.isna(x) or x == '' else 
              ('Completed' if x == 'Completed' else 
               'Refunded' if x == 'Refunded' else 'Unknown')
)

tx_clean['NegativeFlag'] = (tx_clean['Amount'] < 0).astype(int)

tx_clean['YearMonth'] = tx_clean['TranDate'].apply(
    lambda x: f"{x.year}{x.month:02d}"
)

print("Enriching transactions with customer data...")
tx_enriched = tx_clean.merge(
    customers[['CustomerID', 'Name', 'Region', 'SignupDate']],
    on='CustomerID',
    how='left'
)

tx_enriched['Name'] = tx_enriched.apply(
    lambda row: f"Unknown_{row['CustomerID']}" if pd.isna(row['Name']) else row['Name'],
    axis=1
)
tx_enriched['Region'] = tx_enriched['Region'].fillna('Unknown')

print(f"Enriched {len(tx_enriched)} transactions")

print("Aggregating monthly metrics...")
monthly_region = tx_enriched.groupby(['Region', 'YearMonth']).agg(
    NumTrans=('TransactionID', 'count'),
    TotalAmt=('Amount', 'sum'),
    AvgAmt=('Amount', 'mean'),
    NumNegatives=('NegativeFlag', 'sum')
).reset_index()

monthly_region = monthly_region.sort_values(['Region', 'YearMonth'])
print(f"Created {len(monthly_region)} monthly region records")

print("Detecting outliers...")

region_stats = tx_enriched.groupby('Region')['Amount'].agg([
    ('Q1', lambda x: x.quantile(0.25)),
    ('Q3', lambda x: x.quantile(0.75))
]).reset_index()

region_stats['IQR'] = region_stats['Q3'] - region_stats['Q1']

tx_outliers = tx_enriched.merge(region_stats, on='Region', how='left')

def flag_outlier(row):
    if row['Amount'] > (row['Q3'] + 1.5 * row['IQR']):
        return 'High'
    elif row['Amount'] < (row['Q1'] - 1.5 * row['IQR']):
        return 'Low'
    else:
        return 'Normal'

tx_outliers['OutlierFlag'] = tx_outliers.apply(flag_outlier, axis=1)

tx_outliers = tx_outliers.sort_values(['Region', 'Amount'], ascending=[True, False])

print(f"Flagged outliers in {len(tx_outliers)} transactions")
print(f"Outlier distribution:\n{tx_outliers['OutlierFlag'].value_counts()}")

print("\nExporting to CSV...")
tx_outliers.to_csv('tx_outliers.csv', index=False)
monthly_region.to_csv('monthly_region.csv', index=False)

print("\nSample outputs generated successfully!")
print(f"\nFiles created:")
print(f"  - tx_outliers.csv ({len(tx_outliers)} rows)")
print(f"  - monthly_region.csv ({len(monthly_region)} rows)")

print("\n=== Sample from tx_outliers.csv (first 5 rows) ===")
print(tx_outliers.head())

print("\n=== Sample from monthly_region.csv (first 10 rows) ===")
print(monthly_region.head(10))

print("\n=== Region summary ===")
print(tx_outliers.groupby('Region')['OutlierFlag'].value_counts().unstack(fill_value=0))
