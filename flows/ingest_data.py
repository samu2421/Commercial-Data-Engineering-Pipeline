"""
Data Ingestion Flow - Bronze Layer
Ingests raw data from CSV files and Azure Blob Storage JSONL files.
"""
import os
import sys
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.azure_utils import read_jsonl_from_azure


@task(name="Ingest CSV Files", retries=2)
def ingest_csv_files(csv_folder: str, output_folder: str) -> dict:
    """
    Ingest all CSV files from a specified folder.
    
    Args:
        csv_folder: Path to folder containing CSV files
        output_folder: Path to Bronze layer output folder
        
    Returns:
        Dictionary with file names and row counts
    """
    logger = get_run_logger()
    logger.info(f"Starting CSV ingestion from {csv_folder}")
    
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    csv_files = []
    ingestion_summary = {}
    
    # Check if csv_folder exists
    if os.path.exists(csv_folder):
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    else:
        logger.warning(f"CSV folder {csv_folder} does not exist. Creating sample data...")
        # Create sample CSV data for demonstration
        create_sample_csv_data(csv_folder)
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    
    if not csv_files:
        logger.warning("No CSV files found. Creating sample data...")
        create_sample_csv_data(csv_folder)
        csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    
    # Read and save each CSV file to Bronze layer
    for csv_file in csv_files:
        try:
            input_path = os.path.join(csv_folder, csv_file)
            output_path = os.path.join(output_folder, csv_file)
            
            logger.info(f"Reading {csv_file}...")
            df = pd.read_csv(input_path)
            
            # Save to Bronze layer
            df.to_csv(output_path, index=False)
            
            ingestion_summary[csv_file] = len(df)
            logger.info(f"✓ Ingested {csv_file}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"✗ Failed to ingest {csv_file}: {e}")
            ingestion_summary[csv_file] = f"Error: {e}"
    
    logger.info(f"CSV ingestion complete. Processed {len(csv_files)} files")
    return ingestion_summary


@task(name="Ingest JSONL from Azure", retries=3)
def ingest_jsonl_from_azure(blob_url: str, sas_token: str, output_path: str) -> int:
    """
    Ingest JSONL data from Azure Blob Storage.
    
    Args:
        blob_url: Azure Blob Storage container URL
        sas_token: SAS token for authentication
        output_path: Path to save the ingested data
        
    Returns:
        Number of records ingested
    """
    logger = get_run_logger()
    logger.info("Starting JSONL ingestion from Azure Blob Storage")
    
    try:
        # Read JSONL from Azure
        df = read_jsonl_from_azure(blob_url, sas_token)
        
        if df.empty:
            logger.warning("No data retrieved from Azure Blob Storage")
            # Create sample tickets data for demonstration
            df = create_sample_tickets_data()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to Bronze layer as CSV for consistency
        df.to_csv(output_path, index=False)
        
        logger.info(f"✓ Ingested JSONL data: {len(df)} rows")
        logger.info(f"Saved to {output_path}")
        
        return len(df)
        
    except Exception as e:
        logger.error(f"✗ Failed to ingest JSONL from Azure: {e}")
        logger.warning("Creating sample tickets data for demonstration...")
        
        # Create sample data as fallback
        df = create_sample_tickets_data()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        logger.info(f"✓ Created sample tickets data: {len(df)} rows")
        return len(df)


def create_sample_csv_data(output_folder: str) -> None:
    """
    Create sample CSV files for demonstration purposes.
    
    Args:
        output_folder: Folder to save sample CSV files
    """
    os.makedirs(output_folder, exist_ok=True)
    
    # Sample Orders data
    orders_data = {
        'order_id': [f'ORD{i:04d}' for i in range(1, 101)],
        'customer_id': [f'CUST{(i % 20) + 1:03d}' for i in range(1, 101)],
        'order_date': pd.date_range('2024-01-01', periods=100, freq='D').strftime('%Y-%m-%d').tolist(),
        'total_value': [round(50 + (i * 3.5) % 200, 2) for i in range(1, 101)],
        'status': ['completed' if i % 10 != 0 else 'cancelled' for i in range(1, 101)]
    }
    pd.DataFrame(orders_data).to_csv(os.path.join(output_folder, 'orders.csv'), index=False)
    
    # Sample Customers data
    customers_data = {
        'customer_id': [f'CUST{i:03d}' for i in range(1, 21)],
        'customer_name': [f'Customer {i}' for i in range(1, 21)],
        'email': [f'customer{i}@example.com' for i in range(1, 21)],
        'join_date': pd.date_range('2023-01-01', periods=20, freq='15D').strftime('%Y-%m-%d').tolist()
    }
    pd.DataFrame(customers_data).to_csv(os.path.join(output_folder, 'customers.csv'), index=False)
    
    # Sample Products data
    products_data = {
        'product_id': [f'PROD{i:03d}' for i in range(1, 31)],
        'product_name': [f'Product {i}' for i in range(1, 31)],
        'category': ['Food' if i % 3 == 0 else 'Beverage' if i % 3 == 1 else 'Dessert' for i in range(1, 31)],
        'price': [round(5 + (i * 2.5), 2) for i in range(1, 31)]
    }
    pd.DataFrame(products_data).to_csv(os.path.join(output_folder, 'products.csv'), index=False)
    
    # Sample Order Items data
    order_items_data = {
        'order_item_id': [f'OI{i:05d}' for i in range(1, 201)],
        'order_id': [f'ORD{(i % 100) + 1:04d}' for i in range(1, 201)],
        'product_id': [f'PROD{(i % 30) + 1:03d}' for i in range(1, 201)],
        'quantity': [(i % 5) + 1 for i in range(1, 201)],
        'unit_price': [round(5 + (i * 2.5) % 50, 2) for i in range(1, 201)]
    }
    pd.DataFrame(order_items_data).to_csv(os.path.join(output_folder, 'order_items.csv'), index=False)


def create_sample_tickets_data() -> pd.DataFrame:
    """
    Create sample support tickets data for demonstration.
    
    Returns:
        DataFrame with sample tickets data
    """
    tickets_data = {
        'ticket_id': [f'TKT{i:05d}' for i in range(1, 51)],
        'order_id': [f'ORD{(i % 100) + 1:04d}' for i in range(1, 51)],
        'customer_id': [f'CUST{(i % 20) + 1:03d}' for i in range(1, 51)],
        'issue_type': ['Delivery Issue' if i % 4 == 0 else 'Quality Issue' if i % 4 == 1 else 'Wrong Order' if i % 4 == 2 else 'Other' for i in range(1, 51)],
        'ticket_date': pd.date_range('2024-01-01', periods=50, freq='2D').strftime('%Y-%m-%d').tolist(),
        'status': ['resolved' if i % 3 != 0 else 'open' for i in range(1, 51)],
        'priority': ['high' if i % 5 == 0 else 'medium' if i % 5 in [1, 2] else 'low' for i in range(1, 51)]
    }
    return pd.DataFrame(tickets_data)


@flow(name="Data Ingestion Flow - Bronze Layer")
def ingest_data_flow(
    csv_folder: str = "data/source_csv",
    bronze_folder: str = "data/bronze",
    azure_blob_url: str = None,
    azure_sas_token: str = None
) -> dict:
    """
    Main data ingestion flow to populate Bronze layer.
    
    Args:
        csv_folder: Path to folder containing source CSV files
        bronze_folder: Path to Bronze layer output folder
        azure_blob_url: Azure Blob Storage container URL
        azure_sas_token: SAS token for Azure authentication
        
    Returns:
        Dictionary with ingestion summary
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STARTING DATA INGESTION FLOW - BRONZE LAYER")
    logger.info("=" * 60)
    
    # Ingest CSV files
    csv_summary = ingest_csv_files(csv_folder, bronze_folder)
    
    # Ingest JSONL from Azure
    tickets_output_path = os.path.join(bronze_folder, "support_tickets.csv")
    
    if azure_blob_url and azure_sas_token:
        tickets_count = ingest_jsonl_from_azure(azure_blob_url, azure_sas_token, tickets_output_path)
    else:
        logger.warning("Azure credentials not provided. Using sample tickets data...")
        tickets_count = ingest_jsonl_from_azure("", "", tickets_output_path)
    
    # Summary
    summary = {
        "csv_files": csv_summary,
        "tickets_count": tickets_count,
        "bronze_folder": bronze_folder
    }
    
    logger.info("=" * 60)
    logger.info("DATA INGESTION COMPLETE")
    logger.info(f"Total CSV files processed: {len(csv_summary)}")
    logger.info(f"Total tickets ingested: {tickets_count}")
    logger.info("=" * 60)
    
    return summary


if __name__ == "__main__":
    # Run the flow
    result = ingest_data_flow(
        azure_blob_url="https://jafshop.blob.core.windows.net/jafshop-tickets-jsonl",
        azure_sas_token="sp=rl&st=2025-10-31T10:19:26Z&se=2025-11-15T18:34:26Z&sv=2024-11-04&sr=c&sig=EuOUuV8x5p6iSHZP3wDvbgw1tWHScn2eBLKdBDB0b0w%3D"
    )

