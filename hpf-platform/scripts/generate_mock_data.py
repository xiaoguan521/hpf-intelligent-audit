
import duckdb
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Initialize Faker
fake = Faker('zh_CN')
Faker.seed(42)
np.random.seed(42)

def generate_mock_data(num_customers=10000):
    print(f"🚀 Generating {num_customers} customers and related data...")
    
    # 1. Generate Customers with Rich Features
    # Gender distribution: 60% M, 40% F
    genders = np.random.choice(['1', '2'], size=num_customers, p=[0.6, 0.4])
    
    # Occupation types (impacts stability)
    occupations = np.random.choice(
        ['civil_servant', 'teacher', 'doctor', 'engineer', 'business_owner', 'freelancer', 'worker'],
        size=num_customers,
        p=[0.15, 0.10, 0.08, 0.20, 0.12, 0.15, 0.20]
    )
    
    # City tier (1=一线, 2=二线, 3=三四线)
    city_tiers = np.random.choice([1, 2, 3], size=num_customers, p=[0.3, 0.4, 0.3])
    
    # Credit score (300-850, higher is better)
    # Use normal distribution centered at 650
    credit_scores = np.random.normal(loc=650, scale=80, size=num_customers).clip(300, 850).round(0).astype(int)
    
    customers_data = {
        'cust_id': [f'C{str(i).zfill(6)}' for i in range(1, num_customers + 1)],
        'name': [fake.name() for _ in range(num_customers)],
        'age': np.random.randint(22, 60, size=num_customers),
        'gender': genders,
        'occupation': occupations,
        'city_tier': city_tiers,
        'credit_score': credit_scores,
        # Income: Log-normal distribution to simulate real wealth
        'income': np.random.lognormal(mean=9.2, sigma=0.6, size=num_customers).round(2) 
    }
    df_customers = pd.DataFrame(customers_data)
    
    # 2. Generate Contracts (1 contract per customer for simplicity, some with none)
    # 80% customers have loans
    num_loans = int(num_customers * 0.8)
    loan_cust_indices = np.random.choice(range(num_customers), size=num_loans, replace=False)
    loan_cust_ids = df_customers.iloc[loan_cust_indices]['cust_id'].values
    
    start_dates = [fake.date_between(start_date='-2y', end_date='today') for _ in range(num_loans)]
    
    contracts_data = {
        'contract_no': [f'HT{str(i).zfill(8)}' for i in range(1, num_loans + 1)],
        'cust_id': loan_cust_ids,
        'loan_amt': np.random.randint(10, 100, size=num_loans) * 10000.0, # 10w - 100w
        'loan_period': np.random.choice([12, 24, 36, 60, 120, 240, 360], size=num_loans),
        'loan_date': start_dates,
        # Default status logic will be derived from repayments, initial status mostly '01'
        'status': np.random.choice(['01', '02', '03'], size=num_loans, p=[0.90, 0.05, 0.05])
    }
    df_contracts = pd.DataFrame(contracts_data)
    
    # 3. Generate Repayments with Business Logic
    # For each contract, generate monthly plans
    repayment_rows = []
    
    print("⏳ Generating repayment history (this might take a moment)...")
    
    # Build customer lookup for risk calculation
    customer_lookup = df_customers.set_index('cust_id').to_dict('index')
    
    for _, contract in df_contracts.iterrows():
        c_no = contract['contract_no']
        cust_id = contract['cust_id']
        start_date = contract['loan_date']
        periods = min(contract['loan_period'], 24) # Simulate max 24 months of history
        
        # Get customer info
        customer = customer_lookup[cust_id]
        income = customer['income']
        age = customer['age']
        occupation = customer['occupation']
        city_tier = customer['city_tier']
        credit_score = customer['credit_score']
        loan_amt = contract['loan_amt']
        
        # Calculate default probability based on enhanced business rules
        dti = loan_amt / (income + 1)
        
        risk_score = 0.0
        
        # Rule 1: DTI (Debt-to-Income Ratio) - 最重要
        if dti > 5:
            risk_score += 0.35
        elif dti > 3:
            risk_score += 0.15
        
        # Rule 2: Credit Score - 信用分是硬指标
        if credit_score < 550:
            risk_score += 0.30
        elif credit_score < 650:
            risk_score += 0.15
        elif credit_score > 750:
            risk_score -= 0.10  # 高信用降低风险
        
        # Rule 3: Income level
        if income < 5000:
            risk_score += 0.20
        elif income < 8000:
            risk_score += 0.08
        
        # Rule 4: Occupation stability
        stable_jobs = ['civil_servant', 'teacher', 'doctor', 'engineer']
        risky_jobs = ['freelancer', 'business_owner']
        if occupation in stable_jobs:
            risk_score -= 0.12  # 稳定职业降低风险
        elif occupation in risky_jobs:
            risk_score += 0.18  # 不稳定职业提高风险
        
        # Rule 5: City tier (一线城市收入高但生活成本也高)
        if city_tier == 3:
            risk_score += 0.08  # 三四线城市风险略高
        
        # Rule 6: Age
        if age < 25 or age > 55:
            risk_score += 0.10
        
        # Final default probability: base 3% + risk score
        default_prob = min(max(0.03 + risk_score, 0.0), 0.85)
        is_risky = np.random.random() < default_prob
        
        for p in range(1, periods + 1):
            due_date = start_date + timedelta(days=30 * p)
            if due_date > datetime.now().date():
                break
                
            status = '1' # Normal
            actual_date = due_date
            
            # Simulate Overdue (risky people have 40% chance per period)
            if is_risky and np.random.random() < 0.4:
                status = '2'
                actual_date = due_date + timedelta(days=np.random.randint(1, 90))
                if np.random.random() < 0.1: # 10% chance never paid back (bad debt)
                    actual_date = None
            
            repayment_rows.append({
                'contract_no': c_no,
                'period': p,
                'due_date': due_date,
                'actual_date': actual_date,
                'status': status
            })
            
    df_repayments = pd.DataFrame(repayment_rows)
    
    return df_customers, df_contracts, df_repayments

def save_to_duckdb(df_c, df_ht, df_r, db_path='hpf-platform/data/warehouse.duckdb'):
    print(f"\n💾 Saving to DuckDB: {db_path}")
    con = duckdb.connect(db_path)
    
    # Overwrite existing tables
    con.execute("CREATE OR REPLACE TABLE src_customers AS SELECT * FROM df_c")
    con.execute("CREATE OR REPLACE TABLE src_contracts AS SELECT * FROM df_ht")
    con.execute("CREATE OR REPLACE TABLE src_repayments AS SELECT * FROM df_r")
    
    # Verify
    print(f"✅ Saved src_customers: {len(df_c)} rows")
    print(f"✅ Saved src_contracts: {len(df_ht)} rows")
    print(f"✅ Saved src_repayments: {len(df_r)} rows")
    
    con.close()

if __name__ == "__main__":
    # Generate 100k customers for production-grade model
    # This will result in ~80k contracts and ~2M repayment records
    N_CUSTOMERS = 100000
    
    print(f"⚠️  Generating {N_CUSTOMERS} customers - this will take 2-3 minutes...")
    
    customers, contracts, repayments = generate_mock_data(N_CUSTOMERS)
    
    # Ensure directory exists
    os.makedirs('hpf-platform/data', exist_ok=True)
    
    save_to_duckdb(customers, contracts, repayments)
