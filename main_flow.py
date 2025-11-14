"""
Main Orchestration Flow
Orchestrates the entire restaurant analytics pipeline: Bronze → Silver → Gold
"""
import sys
import os
from pathlib import Path
from prefect import flow
from prefect.logging import get_run_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add flows directory to path
sys.path.append(str(Path(__file__).parent))

from flows.ingest_data import ingest_data_flow
from flows.transform_data import transform_data_flow
from flows.analytics import analytics_flow


@flow(name="Restaurant Analytics Pipeline - End to End")
def restaurant_analytics_pipeline(
    csv_folder: str = "data/source_csv",
    bronze_folder: str = "data/bronze",
    silver_folder: str = "data/silver",
    gold_folder: str = "data/gold",
    azure_blob_url: str = None,
    azure_sas_token: str = None
):
    """
    Main orchestration flow for the restaurant analytics pipeline.
    
    This flow orchestrates the complete data pipeline:
    1. Bronze Layer: Ingest raw data from CSV and Azure Blob Storage
    2. Silver Layer: Clean and transform data
    3. Gold Layer: Compute analytics and create domain models
    
    Args:
        csv_folder: Path to folder containing source CSV files
        bronze_folder: Path to Bronze layer output folder
        silver_folder: Path to Silver layer output folder
        gold_folder: Path to Gold layer output folder
        azure_blob_url: Azure Blob Storage container URL
        azure_sas_token: SAS token for Azure authentication
    """
    logger = get_run_logger()
    
    logger.info("=" * 80)
    logger.info("RESTAURANT ANALYTICS PIPELINE - STARTING")
    logger.info("=" * 80)
    logger.info("")
    logger.info("Pipeline Architecture:")
    logger.info("  Bronze Layer → Raw data ingestion (CSV + JSONL)")
    logger.info("  Silver Layer → Data cleaning and transformation")
    logger.info("  Gold Layer   → Analytics and domain models")
    logger.info("")
    logger.info("=" * 80)
    
    # Stage 1: Bronze Layer - Data Ingestion
    logger.info("")
    logger.info("STAGE 1: BRONZE LAYER - DATA INGESTION")
    logger.info("-" * 80)
    
    ingestion_result = ingest_data_flow(
        csv_folder=csv_folder,
        bronze_folder=bronze_folder,
        azure_blob_url=azure_blob_url,
        azure_sas_token=azure_sas_token
    )
    
    logger.info("✓ Bronze layer complete")
    logger.info("")
    
    # Stage 2: Silver Layer - Data Transformation
    logger.info("STAGE 2: SILVER LAYER - DATA TRANSFORMATION")
    logger.info("-" * 80)
    
    transformation_result = transform_data_flow(
        bronze_folder=bronze_folder,
        silver_folder=silver_folder
    )
    
    logger.info("✓ Silver layer complete")
    logger.info("")
    
    # Stage 3: Gold Layer - Analytics
    logger.info("STAGE 3: GOLD LAYER - ANALYTICS")
    logger.info("-" * 80)
    
    analytics_result = analytics_flow(
        silver_folder=silver_folder,
        gold_folder=gold_folder
    )
    
    logger.info("✓ Gold layer complete")
    logger.info("")
    
    # Pipeline Summary
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION COMPLETE ✓")
    logger.info("=" * 80)
    logger.info("")
    logger.info("PIPELINE SUMMARY:")
    logger.info(f"  Bronze Layer: {bronze_folder}")
    logger.info(f"  Silver Layer: {silver_folder}")
    logger.info(f"  Gold Layer:   {gold_folder}")
    logger.info("")
    logger.info("ANALYTICS OUTPUTS:")
    logger.info(f"  • Average Order Value:     {gold_folder}/average_order_value.csv")
    logger.info(f"  • Tickets Per Order:       {gold_folder}/tickets_per_order.csv")
    logger.info(f"  • Restaurant Summary:      {gold_folder}/restaurant_summary.csv")
    logger.info(f"  • Ticket Analytics:        {gold_folder}/ticket_analytics.csv")
    logger.info(f"  • Overall Metrics:         {gold_folder}/overall_metrics.csv")
    logger.info("")
    logger.info("=" * 80)
    
    return {
        "status": "success",
        "ingestion": ingestion_result,
        "transformation": transformation_result,
        "analytics": analytics_result
    }


if __name__ == "__main__":
    """
    Run the complete restaurant analytics pipeline.

    This script can be executed directly to run the entire pipeline:
        python main_flow.py

    The pipeline will:
    1. Ingest data from CSV files and Azure Blob Storage
    2. Clean and transform the data
    3. Generate analytics and insights

    All outputs will be saved to the data/ folder structure.
    """

    # Load Azure credentials from environment variables
    AZURE_BLOB_URL = os.getenv("AZURE_BLOB_URL")
    AZURE_SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN")

    if not AZURE_BLOB_URL or not AZURE_SAS_TOKEN:
        print("ERROR: Azure credentials not found!")
        print("Please create a .env file with AZURE_BLOB_URL and AZURE_SAS_TOKEN")
        print("See .env.example for reference")
        sys.exit(1)

    # Run the pipeline
    result = restaurant_analytics_pipeline(
        azure_blob_url=AZURE_BLOB_URL,
        azure_sas_token=AZURE_SAS_TOKEN
    )

    # Display professional summary with analytics
    print("\n" + "=" * 100)
    print(" " * 25 + "RESTAURANT ANALYTICS PIPELINE - EXECUTION SUMMARY")
    print("=" * 100)

    # Read and display analytics
    import pandas as pd
    import os

    gold_folder = "data/gold"

    # 1. Overall Metrics
    print("\n OVERALL METRICS")
    print("-" * 100)
    try:
        overall_metrics = pd.read_csv(os.path.join(gold_folder, "overall_metrics.csv"))
        for _, row in overall_metrics.iterrows():
            print(f"  Average Order Value (AOV):  ${row['value']:,.2f}")
            print(f"  Total Revenue:              ${row['total_revenue']:,.2f}")
            print(f"  Total Orders:               {row['total_orders']:,}")
    except Exception as e:
        print(f"  Error loading overall metrics: {e}")

    # 2. Ticket Analytics
    print("\nSUPPORT TICKET ANALYTICS BY CATEGORY")
    print("-" * 100)
    try:
        ticket_analytics = pd.read_csv(os.path.join(gold_folder, "ticket_analytics.csv"))
        print(f"  {'Category':<15} {'Ticket Count':>15} {'Percentage':>15}")
        print("  " + "-" * 47)
        for _, row in ticket_analytics.iterrows():
            print(f"  {row['issue_type']:<15} {row['ticket_count']:>15,} {row['percentage']:>14.2f}%")
        print(f"  {'TOTAL':<15} {ticket_analytics['ticket_count'].sum():>15,} {ticket_analytics['percentage'].sum():>14.2f}%")
    except Exception as e:
        print(f"  Error loading ticket analytics: {e}")

    # 3. Top Customers by AOV
    print("\nTOP 10 CUSTOMERS BY AVERAGE ORDER VALUE")
    print("-" * 100)
    try:
        aov_data = pd.read_csv(os.path.join(gold_folder, "average_order_value.csv"))
        top_customers = aov_data.nlargest(10, 'avg_order_value')
        print(f"  {'Customer ID':<40} {'Total Revenue':>15} {'Avg Order Value':>18} {'Total Orders':>15}")
        print("  " + "-" * 92)
        for _, row in top_customers.iterrows():
            print(f"  {row['customer_id']:<40} ${row['total_revenue']:>14,.2f} ${row['avg_order_value']:>17,.2f} {row['total_orders']:>15,}")
    except Exception as e:
        print(f"  Error loading customer analytics: {e}")

    # 4. Tickets Per Order Summary
    print("\nTICKETS PER ORDER SUMMARY")
    print("-" * 100)
    try:
        tickets_per_order = pd.read_csv(os.path.join(gold_folder, "tickets_per_order.csv"))
        avg_tickets = tickets_per_order['num_tickets'].mean()
        orders_with_tickets = (tickets_per_order['num_tickets'] > 0).sum()
        total_orders = len(tickets_per_order)
        ticket_rate = (orders_with_tickets / total_orders) * 100

        print(f"  Average Tickets Per Order:  {avg_tickets:.2f}")
        print(f"  Orders with Tickets:        {orders_with_tickets:,} ({ticket_rate:.1f}%)")
        print(f"  Orders without Tickets:     {total_orders - orders_with_tickets:,} ({100-ticket_rate:.1f}%)")
        print(f"  Total Orders Analyzed:      {total_orders:,}")
    except Exception as e:
        print(f"  Error loading tickets per order: {e}")

    # 5. Output Files Location
    print("\nOUTPUT FILES LOCATION")
    print("-" * 100)
    print(f"  Bronze Layer (Raw Data):        {os.path.abspath('data/bronze')}")
    print(f"  Silver Layer (Cleaned Data):    {os.path.abspath('data/silver')}")
    print(f"  Gold Layer (Analytics):         {os.path.abspath('data/gold')}")

    print("\nANALYTICS FILES GENERATED")
    print("-" * 100)
    analytics_files = [
        ("Average Order Value", "average_order_value.csv"),
        ("Tickets Per Order", "tickets_per_order.csv"),
        ("Restaurant Summary", "restaurant_summary.csv"),
        ("Ticket Analytics", "ticket_analytics.csv"),
        ("Overall Metrics", "overall_metrics.csv")
    ]

    for name, filename in analytics_files:
        filepath = os.path.join(gold_folder, filename)
        if os.path.exists(filepath):
            size = os.path.getsize(filepath) / 1024  # Size in KB
            if size > 1024:
                size_str = f"{size/1024:.2f} MB"
            else:
                size_str = f"{size:.2f} KB"
            print(f"  ✓ {name:<30} {filename:<35} ({size_str})")
        else:
            print(f"  ✗ {name:<30} {filename:<35} (Not Found)")

    print("\n" + "=" * 100)
    print(" " * 30 + "PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    print("=" * 100)
    print()
