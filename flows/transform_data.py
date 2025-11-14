"""
Data Transformation Flow - Silver Layer
Cleans and transforms raw data from Bronze to Silver layer.
"""
import os
import sys
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.transformations import (
    clean_column_names,
    handle_missing_values,
    standardize_date_columns,
    remove_duplicates,
    clean_orders_data,
    clean_tickets_data,
    validate_data_quality
)


@task(name="Clean Generic CSV Data")
def clean_generic_csv(bronze_folder: str, silver_folder: str, filename: str) -> str:
    """
    Clean and transform any CSV data from Bronze to Silver layer.

    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder
        filename: Name of the CSV file to clean

    Returns:
        Path to cleaned file
    """
    logger = get_run_logger()
    logger.info(f"Cleaning {filename}...")

    # Read data from Bronze
    input_path = os.path.join(bronze_folder, filename)

    if not os.path.exists(input_path):
        logger.warning(f"File not found: {input_path}")
        return None

    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} rows from Bronze layer")

    # Clean the data
    df = clean_column_names(df)

    # Standardize date columns (any column with 'date' or 'at' in name)
    date_cols = [col for col in df.columns if 'date' in col.lower() or col.endswith('_at')]
    if date_cols:
        df = standardize_date_columns(df, date_cols)

    # Handle numeric columns
    numeric_cols = ['subtotal', 'tax_paid', 'order_total', 'price', 'cost', 'tax_rate']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove duplicates based on ID column if exists
    id_cols = [col for col in df.columns if col == 'id' or col.endswith('_id')]
    if id_cols:
        df = remove_duplicates(df, subset=[id_cols[0]])

    # Handle missing values
    df = handle_missing_values(df)

    # Save to Silver layer
    os.makedirs(silver_folder, exist_ok=True)
    output_filename = filename.replace('.csv', '_clean.csv')
    output_path = os.path.join(silver_folder, output_filename)
    df.to_csv(output_path, index=False)

    logger.info(f"✓ Cleaned {filename} saved: {len(df)} rows → {output_filename}")
    return output_path


@task(name="Clean Orders Data")
def clean_orders(bronze_folder: str, silver_folder: str) -> str:
    """
    Clean and transform orders data from Bronze to Silver layer.

    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder

    Returns:
        Path to cleaned orders file
    """
    logger = get_run_logger()
    logger.info("Cleaning orders data...")

    # Read orders data from Bronze
    orders_path = os.path.join(bronze_folder, "orders.csv")

    if not os.path.exists(orders_path):
        logger.error(f"Orders file not found: {orders_path}")
        raise FileNotFoundError(f"Orders file not found: {orders_path}")

    df = pd.read_csv(orders_path)
    logger.info(f"Loaded {len(df)} orders from Bronze layer")

    # Clean the data
    df = clean_orders_data(df)

    # Additional cleaning specific to orders
    # Ensure numeric columns are properly typed
    numeric_cols = ['subtotal', 'tax_paid', 'order_total']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0)

    # Validate data quality
    required_columns = ['id']
    if not validate_data_quality(df, required_columns):
        raise ValueError("Orders data quality validation failed")

    # Save to Silver layer
    os.makedirs(silver_folder, exist_ok=True)
    output_path = os.path.join(silver_folder, "orders_clean.csv")
    df.to_csv(output_path, index=False)

    logger.info(f"✓ Cleaned orders data saved: {len(df)} rows")
    return output_path


@task(name="Clean Customers Data")
def clean_customers(bronze_folder: str, silver_folder: str) -> str:
    """
    Clean and transform customers data from Bronze to Silver layer.
    
    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder
        
    Returns:
        Path to cleaned customers file
    """
    logger = get_run_logger()
    logger.info("Cleaning customers data...")
    
    # Read customers data from Bronze
    customers_path = os.path.join(bronze_folder, "customers.csv")
    
    if not os.path.exists(customers_path):
        logger.warning(f"Customers file not found: {customers_path}")
        return None
    
    df = pd.read_csv(customers_path)
    logger.info(f"Loaded {len(df)} customers from Bronze layer")
    
    # Clean the data
    df = clean_column_names(df)
    
    # Standardize date columns
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    df = standardize_date_columns(df, date_cols)
    
    # Remove duplicates
    if 'customer_id' in df.columns:
        df = remove_duplicates(df, subset=['customer_id'])
    
    # Handle missing values
    df = handle_missing_values(df)
    
    # Save to Silver layer
    os.makedirs(silver_folder, exist_ok=True)
    output_path = os.path.join(silver_folder, "customers_clean.csv")
    df.to_csv(output_path, index=False)
    
    logger.info(f"✓ Cleaned customers data saved: {len(df)} rows")
    return output_path


@task(name="Clean Products Data")
def clean_products(bronze_folder: str, silver_folder: str) -> str:
    """
    Clean and transform products data from Bronze to Silver layer.
    
    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder
        
    Returns:
        Path to cleaned products file
    """
    logger = get_run_logger()
    logger.info("Cleaning products data...")
    
    # Read products data from Bronze
    products_path = os.path.join(bronze_folder, "products.csv")
    
    if not os.path.exists(products_path):
        logger.warning(f"Products file not found: {products_path}")
        return None
    
    df = pd.read_csv(products_path)
    logger.info(f"Loaded {len(df)} products from Bronze layer")
    
    # Clean the data
    df = clean_column_names(df)
    
    # Remove duplicates
    if 'product_id' in df.columns:
        df = remove_duplicates(df, subset=['product_id'])
    
    # Handle missing values
    df = handle_missing_values(df)
    
    # Ensure price is numeric
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['price'] = df['price'].fillna(0)
    
    # Save to Silver layer
    os.makedirs(silver_folder, exist_ok=True)
    output_path = os.path.join(silver_folder, "products_clean.csv")
    df.to_csv(output_path, index=False)
    
    logger.info(f"✓ Cleaned products data saved: {len(df)} rows")
    return output_path


@task(name="Clean Order Items Data")
def clean_order_items(bronze_folder: str, silver_folder: str) -> str:
    """
    Clean and transform order items data from Bronze to Silver layer.
    
    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder
        
    Returns:
        Path to cleaned order items file
    """
    logger = get_run_logger()
    logger.info("Cleaning order items data...")
    
    # Read order items data from Bronze
    order_items_path = os.path.join(bronze_folder, "order_items.csv")
    
    if not os.path.exists(order_items_path):
        logger.warning(f"Order items file not found: {order_items_path}")
        return None
    
    df = pd.read_csv(order_items_path)
    logger.info(f"Loaded {len(df)} order items from Bronze layer")
    
    # Clean the data
    df = clean_column_names(df)
    
    # Remove duplicates
    if 'order_item_id' in df.columns:
        df = remove_duplicates(df, subset=['order_item_id'])
    
    # Handle missing values
    df = handle_missing_values(df)
    
    # Ensure numeric columns are properly typed
    numeric_cols = ['quantity', 'unit_price']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0)
    
    # Calculate line total if not present
    if 'quantity' in df.columns and 'unit_price' in df.columns:
        df['line_total'] = df['quantity'] * df['unit_price']
    
    # Save to Silver layer
    os.makedirs(silver_folder, exist_ok=True)
    output_path = os.path.join(silver_folder, "order_items_clean.csv")
    df.to_csv(output_path, index=False)
    
    logger.info(f"✓ Cleaned order items data saved: {len(df)} rows")
    return output_path


@task(name="Clean Support Tickets Data")
def clean_tickets(bronze_folder: str, silver_folder: str) -> str:
    """
    Clean and transform support tickets data from Bronze to Silver layer.
    
    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder
        
    Returns:
        Path to cleaned tickets file
    """
    logger = get_run_logger()
    logger.info("Cleaning support tickets data...")
    
    # Read tickets data from Bronze
    tickets_path = os.path.join(bronze_folder, "support_tickets.csv")
    
    if not os.path.exists(tickets_path):
        logger.error(f"Tickets file not found: {tickets_path}")
        raise FileNotFoundError(f"Tickets file not found: {tickets_path}")
    
    df = pd.read_csv(tickets_path)
    logger.info(f"Loaded {len(df)} tickets from Bronze layer")
    
    # Clean the data
    df = clean_tickets_data(df)
    
    # Validate data quality
    required_columns = ['ticket_id']
    if not validate_data_quality(df, required_columns):
        raise ValueError("Tickets data quality validation failed")
    
    # Save to Silver layer
    os.makedirs(silver_folder, exist_ok=True)
    output_path = os.path.join(silver_folder, "support_tickets_clean.csv")
    df.to_csv(output_path, index=False)
    
    logger.info(f"✓ Cleaned support tickets data saved: {len(df)} rows")
    return output_path


@flow(name="Data Transformation Flow - Silver Layer")
def transform_data_flow(
    bronze_folder: str = "data/bronze",
    silver_folder: str = "data/silver"
) -> dict:
    """
    Main data transformation flow to populate Silver layer.
    Dynamically processes all CSV files from Bronze layer.

    Args:
        bronze_folder: Path to Bronze layer folder
        silver_folder: Path to Silver layer folder

    Returns:
        Dictionary with transformation summary
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STARTING DATA TRANSFORMATION FLOW - SILVER LAYER")
    logger.info("=" * 60)

    # Get all CSV files from Bronze layer
    csv_files = [f for f in os.listdir(bronze_folder) if f.endswith('.csv')]
    logger.info(f"Found {len(csv_files)} CSV files to process")

    summary = {}

    # Process each CSV file
    for csv_file in csv_files:
        # Special handling for support tickets (from Azure)
        if csv_file == "support_tickets.csv":
            tickets_path = clean_tickets(bronze_folder, silver_folder)
            summary["support_tickets"] = tickets_path
        # Special handling for orders (has specific validation)
        elif csv_file == "orders.csv":
            orders_path = clean_orders(bronze_folder, silver_folder)
            summary["orders"] = orders_path
        # Generic cleaning for all other files
        else:
            cleaned_path = clean_generic_csv(bronze_folder, silver_folder, csv_file)
            file_key = csv_file.replace('.csv', '')
            summary[file_key] = cleaned_path

    summary["silver_folder"] = silver_folder

    logger.info("=" * 60)
    logger.info("DATA TRANSFORMATION COMPLETE")
    logger.info(f"Cleaned datasets saved to: {silver_folder}")
    logger.info(f"Total files processed: {len(csv_files)}")
    logger.info("=" * 60)

    return summary


if __name__ == "__main__":
    # Run the flow
    result = transform_data_flow()

