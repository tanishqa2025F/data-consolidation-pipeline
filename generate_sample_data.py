"""
generate_sample_data.py
Generates synthetic messy Excel files simulating real-world HVAC/mechanical services data
from multiple disconnected systems — mirrors the data consolidation challenges 
commonly found in companies transitioning from manual to automated data processes.

Run this first to create the sample data files in data/raw/
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

np.random.seed(42)

# --- Configuration ---
NUM_ORDERS = 200
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31)

TECHNICIANS = [
    "Mike Johnson", "Sarah Chen", "David Martinez", "Emily Brown",
    "James Wilson", "Lisa Anderson", "Robert Taylor", "Maria Garcia",
    "Tom Davis", "Jennifer Lee"
]

CUSTOMERS = [
    "Richmond Medical Center", "VCU Health System", "Henrico County Schools",
    "Dominion Energy HQ", "Capital One Arena", "Virginia Museum of Fine Arts",
    "James River Corporate Park", "Westham Station Offices", "Bon Secours Hospital",
    "Stony Point Fashion Park", "Reynolds Crossing", "Innsbrook Office Complex",
    "Short Pump Town Center", "Regency Square Mall", "Willow Lawn Shopping Center"
]

LOCATIONS = [
    "Richmond - Downtown", "Richmond - West End", "Henrico", "Chesterfield",
    "Glen Allen", "Midlothian", "Short Pump", "Mechanicsville"
]

# Service types — intentionally inconsistent across systems
SERVICE_TYPES_A = ["HVAC Repair", "HVAC Install", "Plumbing Repair", "Plumbing Install", 
                   "Preventive Maintenance", "Emergency"]
SERVICE_TYPES_B = ["HVAC - Repair", "Install - HVAC", "Plumbing - Repair", "Plumbing Installation",
                   "PM", "Emergency Call"]
SERVICE_TYPES_MANUAL = ["Repair - HVAC", "HVAC Installation", "Plumbing Repair", "Plumbing Install",
                        "Maintenance", "Emergency Service"]

STATUS_A = ["Complete", "In Progress", "Pending", "Cancelled"]
STATUS_B = ["Completed", "WIP", "Open", "Canceled"]


def random_dates(n):
    """Generate n random dates between START_DATE and END_DATE."""
    date_range = (END_DATE - START_DATE).days
    return [START_DATE + timedelta(days=np.random.randint(0, date_range)) for _ in range(n)]


def generate_system_a(n):
    """
    System A: Service order management system.
    Simulates a more structured database export with some quality issues.
    """
    dates = random_dates(n)
    data = {
        "Order ID": [f"SO-{1000 + i}" for i in range(n)],
        "Issue Date": [d.strftime("%m/%d/%Y") for d in dates],  # US date format
        "Customer Name": np.random.choice(CUSTOMERS, n),
        "Service Type": np.random.choice(SERVICE_TYPES_A, n),
        "Technician Assigned": np.random.choice(TECHNICIANS, n),
        "Total Charged": np.round(np.random.uniform(150, 15000, n), 2),
        "Status": np.random.choice(STATUS_A, n, p=[0.6, 0.15, 0.15, 0.1]),
        "Location": np.random.choice(LOCATIONS, n),
    }
    
    df = pd.DataFrame(data)
    
    # --- Introduce realistic data quality issues ---
    
    # 1. Some missing values (5-8% nulls scattered)
    for col in ["Customer Name", "Location", "Total Charged"]:
        mask = np.random.random(n) < 0.06
        df.loc[mask, col] = np.nan
    
    # 2. Inconsistent name formatting (some lowercase, extra spaces)
    name_issues = np.random.random(n) < 0.1
    df.loc[name_issues, "Technician Assigned"] = df.loc[name_issues, "Technician Assigned"].apply(
        lambda x: f"  {x.lower()}  " if pd.notna(x) else x
    )
    
    # 3. Duplicate rows (3 exact duplicates)
    dupes = df.sample(3)
    df = pd.concat([df, dupes], ignore_index=True)
    
    # 4. One negative revenue value (data entry error)
    df.loc[5, "Total Charged"] = -450.00
    
    return df


def generate_system_b(n):
    """
    System B: Technician time tracking system.
    Different column names, different date format, different status values.
    """
    dates = random_dates(n)
    hours = np.round(np.random.uniform(1, 10, n), 1)
    
    data = {
        "Tech Name": np.random.choice(TECHNICIANS, n),
        "Report Date": [d.strftime("%Y-%m-%d") for d in dates],  # ISO format (different from System A)
        "Hrs Worked": hours,
        "OT Hours": np.where(hours > 8, np.round(hours - 8, 1), 0),
        "Pay Rate": np.random.choice([28.50, 32.00, 35.75, 42.00, 55.00], n),
        "Job Type": np.random.choice(SERVICE_TYPES_B, n),
        "Completion": np.random.choice(STATUS_B, n, p=[0.65, 0.15, 0.12, 0.08]),
        "Site": np.random.choice(LOCATIONS, n),
    }
    
    df = pd.DataFrame(data)
    
    # Data quality issues
    # 1. Some tech names have middle initials sometimes
    name_variants = np.random.random(n) < 0.08
    df.loc[name_variants, "Tech Name"] = df.loc[name_variants, "Tech Name"].apply(
        lambda x: x.split()[0] + " J. " + x.split()[-1] if pd.notna(x) and len(x.split()) == 2 else x
    )
    
    # 2. Missing hours (null values)
    mask = np.random.random(n) < 0.04
    df.loc[mask, "Hrs Worked"] = np.nan
    
    # 3. A few impossible values (negative hours, 25-hour days)
    df.loc[2, "Hrs Worked"] = -2.0
    df.loc[15, "Hrs Worked"] = 25.5
    
    return df


def generate_manual_excel(n):
    """
    System C: Manual Excel tracker maintained by office staff.
    ALL CAPS headers, abbreviated columns, inconsistent data entry.
    This is the messiest source — simulates a real manually-maintained spreadsheet.
    """
    dates = random_dates(n)
    
    data = {
        "DATE": [d.strftime("%m-%d-%y") for d in dates],  # Yet another date format
        "CUST": np.random.choice(CUSTOMERS, n),
        "TECH": np.random.choice(TECHNICIANS, n),
        "SERVICE": np.random.choice(SERVICE_TYPES_MANUAL, n),
        "AMT": np.random.choice(
            # Mix of formats: some with $, some without, some with commas
            [f"${np.random.uniform(200, 12000):.2f}",
             f"{np.random.uniform(200, 12000):.2f}",
             f"${np.random.uniform(200, 12000):,.2f}"],
            n
        ),
        "SITE": np.random.choice(LOCATIONS, n),
        "NOTES": np.random.choice(
            ["", "Callback needed", "Parts on order", "Customer satisfied", 
             "Warranty claim", "Referred by existing client", np.nan], n
        ),
    }
    
    df = pd.DataFrame(data)
    
    # More quality issues
    # 1. Customer name abbreviations/typos
    typo_mask = np.random.random(n) < 0.1
    df.loc[typo_mask, "CUST"] = df.loc[typo_mask, "CUST"].apply(
        lambda x: x[:15] + "..." if pd.notna(x) and len(str(x)) > 15 else x
    )
    
    # 2. Some completely empty rows (someone hit enter too many times)
    empty_rows = pd.DataFrame({col: [np.nan] for col in df.columns})
    df = pd.concat([df.iloc[:50], empty_rows, empty_rows, df.iloc[50:]], ignore_index=True)
    
    return df


def main():
    """Generate all three messy data sources and save to Excel."""
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate data
    print("Generating synthetic data from 3 simulated business systems...\n")
    
    system_a = generate_system_a(NUM_ORDERS)
    print(f"System A (Service Orders):    {len(system_a)} rows, {system_a.isnull().sum().sum()} null values, "
          f"{system_a.duplicated().sum()} duplicates")
    
    system_b = generate_system_b(int(NUM_ORDERS * 1.5))
    print(f"System B (Technician Hours):  {len(system_b)} rows, {system_b.isnull().sum().sum()} null values")
    
    system_c = generate_manual_excel(int(NUM_ORDERS * 0.8))
    print(f"System C (Manual Revenue):    {len(system_c)} rows, {system_c.isnull().sum().sum()} null values")
    
    # Save to Excel
    system_a.to_excel(os.path.join(output_dir, "service_orders_system_a.xlsx"), index=False)
    system_b.to_excel(os.path.join(output_dir, "technician_hours_system_b.xlsx"), index=False)
    system_c.to_excel(os.path.join(output_dir, "revenue_tracking_manual.xlsx"), index=False)
    
    print(f"\nFiles saved to {output_dir}/")
    print("  - service_orders_system_a.xlsx")
    print("  - technician_hours_system_b.xlsx")
    print("  - revenue_tracking_manual.xlsx")


if __name__ == "__main__":
    main()
