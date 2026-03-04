"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: trip_id
    type: string
    description: Unique identifier for the trip (vendor_id + pickup_datetime + dropoff_datetime hash)
    primary_key: true
  - name: vendor_id
    type: integer
    description: Identifier for the taxi vendor/company
  - name: pickup_datetime
    type: timestamp
    description: Date and time when the trip started
  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the trip ended
  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: double
    description: Trip distance in miles
  - name: rate_code_id
    type: integer
    description: Rate code category for the trip
  - name: store_and_fwd_flag
    type: string
    description: Flag indicating if trip data was stored and forwarded (Y/N)
  - name: pickup_location_id
    type: integer
    description: TLC zone ID for pickup location
  - name: dropoff_location_id
    type: integer
    description: TLC zone ID for dropoff location
  - name: payment_type
    type: integer
    description: Payment method used (references payment_lookup table)
  - name: fare_amount
    type: double
    description: Base fare
  - name: extra
    type: double
    description: Miscellaneous extra charges
  - name: mta_tax
    type: double
    description: MTA tax amount
  - name: tip_amount
    type: double
    description: Tip amount (may be zero for cash payments)
  - name: tolls_amount
    type: double
    description: Toll charges
  - name: total_amount
    type: double
    description: Total trip fare
  - name: taxi_type
    type: string
    description: Type of taxi (yellow, green, fhv, etc.)
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted from TLC

@bruin"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import requests


# TLC Data endpoint based on taxi type
TLC_BASE_URL = "https://d37cibb9cSandBox.cloudfront.net"


def get_bruin_context():
    """
    Extract Bruin runtime context from environment variables.
    Returns: dict with start_date, end_date, and taxi_types.
    """
    start_date = os.getenv("BRUIN_START_DATE", "2024-01-01")
    end_date = os.getenv("BRUIN_END_DATE")
    
    # End date defaults to today if not set
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Parse pipeline variables from BRUIN_VARS
    bruin_vars_json = os.getenv("BRUIN_VARS", "{}")
    bruin_vars = json.loads(bruin_vars_json)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    
    return {
        "start_date": start_date,
        "end_date": end_date,
        "taxi_types": taxi_types,
    }


def generate_trip_id(row):
    """Generate a unique trip ID as a hash of vendor_id + pickup_datetime + dropoff_datetime."""
    try:
        key = f"{int(row['vendor_id'])}{row['tpep_pickup_datetime']}{row['tpep_dropoff_datetime']}"
        return hashlib.md5(key.encode()).hexdigest()
    except (KeyError, ValueError, TypeError):
        # Fallback for Green taxi or if columns are missing
        try:
            key = f"{int(row.get('vendor_id', 0))}{row.get('lpep_pickup_datetime', '')}{row.get('lpep_dropoff_datetime', '')}"
            return hashlib.md5(key.encode()).hexdigest()
        except:
            return None


def fetch_tlc_data(taxi_type: str, year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Fetch NYC TLC trip data for a specific taxi type, year, and month.
    
    Args:
        taxi_type: yellow, green, fhv, etc.
        year: 4-digit year
        month: 1-12 month
        
    Returns:
        DataFrame or None if data unavailable
    """
    # Format month to zero-padded string
    month_str = f"{month:02d}"
    
    # Construct parquet URL
    url = f"{TLC_BASE_URL}/{taxi_type}_tripdata_{year}-{month_str}.parquet"
    
    try:
        print(f"Fetching: {url}")
        df = pd.read_parquet(url)
        print(f"Downloaded {len(df)} records for {taxi_type} taxi, {year}-{month_str}")
        return df
    except Exception as e:
        print(f"Warning: Could not fetch {url}: {str(e)}")
        return None


def normalize_columns(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """
    Normalize columns for different taxi types to a common schema.
    Yellow and Green have different column names.
    """
    df = df.copy()
    
    # Map pickup/dropoff datetime columns based on taxi type
    if taxi_type == "yellow":
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
    elif taxi_type == "green":
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
    else:
        pickup_col = "pickup_datetime"
        dropoff_col = "dropoff_datetime"
    
    # Rename columns to standard names if they exist
    column_mapping = {}
    if pickup_col in df.columns:
        column_mapping[pickup_col] = "pickup_datetime"
    if dropoff_col in df.columns:
        column_mapping[dropoff_col] = "dropoff_datetime"
    
    df = df.rename(columns=column_mapping)
    
    # Standardize payment_type to integer if it's a string
    if "payment_type" in df.columns:
        df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").fillna(0).astype(int)
    
    return df


def select_and_rename_columns(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """
    Select and rename columns to match the asset schema.
    """
    column_mapping = {
        "vendor_id": "vendor_id",
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "rate_code_id": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "pickup_location_id": "pickup_location_id",
        "dropoff_location_id": "dropoff_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "total_amount": "total_amount",
    }
    
    df = df.copy()
    
    # Rename columns that exist
    existing_mappings = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_mappings)
    
    # Select only the columns we need (those that now exist in the dataframe)
    output_columns = [
        "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
        "trip_distance", "rate_code_id", "store_and_fwd_flag", "pickup_location_id",
        "dropoff_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "total_amount"
    ]
    
    # Filter to columns that exist
    output_columns = [col for col in output_columns if col in df.columns]
    df = df[output_columns]
    
    return df


def materialize():
    """
    Fetch NYC TLC trip data for the specified date range and taxi types.
    Uses Bruin runtime context (date window, taxi_types variable).
    """
    # Get Bruin context
    context = get_bruin_context()
    start_date = datetime.strptime(context["start_date"], "%Y-%m-%d")
    end_date = datetime.strptime(context["end_date"], "%Y-%m-%d")
    taxi_types = context["taxi_types"]
    
    print(f"Ingestion window: {context['start_date']} to {context['end_date']}")
    print(f"Taxi types: {taxi_types}")
    
    all_dfs = []
    
    # Generate list of (year, month) tuples for the date range
    current = start_date
    while current <= end_date:
        data = fetch_tlc_data(taxi_types[0] if isinstance(taxi_types, list) else taxi_types, current.year, current.month)
        if data is not None:
            # Normalize and transform
            data = normalize_columns(data, taxi_types[0] if isinstance(taxi_types, list) else taxi_types)
            data = select_and_rename_columns(data, taxi_types[0] if isinstance(taxi_types, list) else taxi_types)
            
            # Add taxi_type column
            data["taxi_type"] = taxi_types[0] if isinstance(taxi_types, list) else taxi_types
            
            # Add trip_id
            data["trip_id"] = data.apply(generate_trip_id, axis=1)
            
            # Add extracted_at timestamp
            data["extracted_at"] = datetime.now()
            
            all_dfs.append(data)
        
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    if not all_dfs:
        print("Warning: No data fetched for the specified date range and taxi types.")
        # Return empty DataFrame with correct schema
        return pd.DataFrame({
            "trip_id": pd.Series(dtype="string"),
            "vendor_id": pd.Series(dtype="int32"),
            "pickup_datetime": pd.Series(dtype="datetime64[ns]"),
            "dropoff_datetime": pd.Series(dtype="datetime64[ns]"),
            "passenger_count": pd.Series(dtype="int32"),
            "trip_distance": pd.Series(dtype="float64"),
            "rate_code_id": pd.Series(dtype="int32"),
            "store_and_fwd_flag": pd.Series(dtype="string"),
            "pickup_location_id": pd.Series(dtype="int32"),
            "dropoff_location_id": pd.Series(dtype="int32"),
            "payment_type": pd.Series(dtype="int32"),
            "fare_amount": pd.Series(dtype="float64"),
            "extra": pd.Series(dtype="float64"),
            "mta_tax": pd.Series(dtype="float64"),
            "tip_amount": pd.Series(dtype="float64"),
            "tolls_amount": pd.Series(dtype="float64"),
            "total_amount": pd.Series(dtype="float64"),
            "taxi_type": pd.Series(dtype="string"),
            "extracted_at": pd.Series(dtype="datetime64[ns]"),
        })
    
    # Concatenate all data
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure correct column order
    column_order = [
        "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
        "trip_distance", "rate_code_id", "store_and_fwd_flag", "pickup_location_id",
        "dropoff_location_id", "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "total_amount", "taxi_type", "extracted_at"
    ]
    final_df = final_df[column_order]
    
    print(f"Total records fetched: {len(final_df)}")
    return final_df


