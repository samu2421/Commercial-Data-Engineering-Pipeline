"""
Analytics Flow - Gold Layer
Computes analytics metrics and creates domain-oriented models.
"""
import os
import sys
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.transformations import merge_datasets


@task(name="Calculate Average Order Value")
def calculate_aov(silver_folder: str, gold_folder: str) -> str:
    """
    Calculate Average Order Value (AOV) by customer and overall.
    
    Args:
        silver_folder: Path to Silver layer folder
        gold_folder: Path to Gold layer folder
        
    Returns:
        Path to AOV analytics file
    """
    logger = get_run_logger()
    logger.info("Calculating Average Order Value (AOV)...")
    
    # Read cleaned orders data - try multiple possible filenames
    possible_filenames = ["orders_clean.csv", "raw_orders_clean.csv"]
    orders_path = None

    for filename in possible_filenames:
        test_path = os.path.join(silver_folder, filename)
        if os.path.exists(test_path):
            orders_path = test_path
            break

    if not orders_path:
        logger.error(f"Orders file not found. Tried: {possible_filenames}")
        raise FileNotFoundError(f"Orders file not found in {silver_folder}")
    
    orders_df = pd.read_csv(orders_path)
    logger.info(f"Loaded {len(orders_df)} orders")
    
    # Identify the value column (order_total, subtotal, total_value, etc.)
    value_col = None
    for col_name in ['order_total', 'total_value', 'subtotal', 'total', 'amount']:
        if col_name in orders_df.columns:
            value_col = col_name
            break

    # Identify the customer column
    customer_col = None
    for col_name in ['customer', 'customer_id', 'customer_external_id']:
        if col_name in orders_df.columns:
            customer_col = col_name
            break

    if not value_col or not customer_col:
        logger.error(f"Required columns not found. Available: {orders_df.columns.tolist()}")
        raise ValueError("Cannot find required columns for AOV calculation")

    logger.info(f"Using columns: customer='{customer_col}', value='{value_col}'")

    # Calculate AOV by customer
    aov_by_customer = orders_df.groupby(customer_col).agg({
        value_col: ['sum', 'mean', 'count']
    }).reset_index()

    # Flatten column names
    aov_by_customer.columns = ['customer_id', 'total_revenue', 'avg_order_value', 'total_orders']

    # Round to 2 decimal places
    aov_by_customer['total_revenue'] = aov_by_customer['total_revenue'].round(2)
    aov_by_customer['avg_order_value'] = aov_by_customer['avg_order_value'].round(2)

    # Calculate overall AOV
    overall_aov = orders_df[value_col].mean()
    total_revenue = orders_df[value_col].sum()
    total_orders = len(orders_df)

    logger.info(f"Overall AOV: ${overall_aov:.2f}")
    logger.info(f"Total Revenue: ${total_revenue:.2f}")
    logger.info(f"Total Orders: {total_orders}")

    # Save to Gold layer
    os.makedirs(gold_folder, exist_ok=True)
    output_path = os.path.join(gold_folder, "average_order_value.csv")
    aov_by_customer.to_csv(output_path, index=False)

    # Also save overall metrics
    overall_metrics = pd.DataFrame([{
        'metric': 'Overall AOV',
        'value': round(overall_aov, 2),
        'total_revenue': round(total_revenue, 2),
        'total_orders': total_orders
    }])
    overall_path = os.path.join(gold_folder, "overall_metrics.csv")
    overall_metrics.to_csv(overall_path, index=False)

    logger.info(f"✓ AOV analytics saved: {len(aov_by_customer)} customers")
    return output_path


@task(name="Calculate Tickets Per Order")
def calculate_tickets_per_order(silver_folder: str, gold_folder: str) -> str:
    """
    Calculate number of support tickets per order.
    
    Args:
        silver_folder: Path to Silver layer folder
        gold_folder: Path to Gold layer folder
        
    Returns:
        Path to tickets per order analytics file
    """
    logger = get_run_logger()
    logger.info("Calculating tickets per order...")
    
    # Read cleaned data - try multiple possible filenames
    possible_orders = ["orders_clean.csv", "raw_orders_clean.csv"]
    orders_path = None
    for filename in possible_orders:
        test_path = os.path.join(silver_folder, filename)
        if os.path.exists(test_path):
            orders_path = test_path
            break

    if not orders_path:
        logger.error(f"Orders file not found. Tried: {possible_orders}")
        raise FileNotFoundError(f"Orders file not found in {silver_folder}")

    tickets_path = os.path.join(silver_folder, "support_tickets_clean.csv")
    if not os.path.exists(tickets_path):
        logger.error(f"Tickets file not found: {tickets_path}")
        raise FileNotFoundError(f"Tickets file not found: {tickets_path}")
    
    orders_df = pd.read_csv(orders_path)
    tickets_df = pd.read_csv(tickets_path)
    
    logger.info(f"Loaded {len(orders_df)} orders and {len(tickets_df)} tickets")
    
    # Count tickets per order
    if 'order_id' in tickets_df.columns:
        tickets_count = tickets_df.groupby('order_id').size().reset_index(name='num_tickets')

        # Identify the order ID column in orders (could be 'id' or 'order_id')
        order_id_col = 'id' if 'id' in orders_df.columns else 'order_id'

        if order_id_col in orders_df.columns:
            # Merge with orders to include orders with no tickets
            result = orders_df.merge(tickets_count, left_on=order_id_col, right_on='order_id', how='left')

            # Fill NaN with 0 for orders with no tickets
            result['num_tickets'] = result['num_tickets'].fillna(0).astype(int)

            # Select relevant columns
            columns_to_keep = [order_id_col, 'num_tickets']

            # Add customer column if exists
            customer_col = 'customer' if 'customer' in result.columns else 'customer_id'
            if customer_col in result.columns:
                columns_to_keep.insert(1, customer_col)

            # Add value column if exists
            value_col = next((col for col in ['order_total', 'total_value', 'subtotal'] if col in result.columns), None)
            if value_col:
                columns_to_keep.append(value_col)

            # Add date column if exists
            date_col = next((col for col in ['ordered_at', 'order_date', 'created_at'] if col in result.columns), None)
            if date_col:
                columns_to_keep.append(date_col)

            result = result[[col for col in columns_to_keep if col in result.columns]]

            # Calculate summary statistics
            avg_tickets_per_order = result['num_tickets'].mean()
            orders_with_tickets = (result['num_tickets'] > 0).sum()
            ticket_rate = (orders_with_tickets / len(result)) * 100

            logger.info(f"Average tickets per order: {avg_tickets_per_order:.2f}")
            logger.info(f"Orders with tickets: {orders_with_tickets} ({ticket_rate:.1f}%)")

            # Save to Gold layer
            os.makedirs(gold_folder, exist_ok=True)
            output_path = os.path.join(gold_folder, "tickets_per_order.csv")
            result.to_csv(output_path, index=False)

            logger.info(f"✓ Tickets per order analytics saved: {len(result)} orders")
            return output_path
        else:
            logger.error("order ID column not found in orders data")
            raise ValueError("order ID column not found in orders data")
    else:
        logger.error("order_id column not found in tickets data")
        raise ValueError("order_id column not found in tickets data")


@task(name="Create Restaurant Summary")
def create_restaurant_summary(silver_folder: str, gold_folder: str) -> str:
    """
    Create a comprehensive restaurant summary table merging orders and tickets.
    
    Args:
        silver_folder: Path to Silver layer folder
        gold_folder: Path to Gold layer folder
        
    Returns:
        Path to restaurant summary file
    """
    logger = get_run_logger()
    logger.info("Creating restaurant summary...")
    
    # Read cleaned data - try multiple possible filenames
    possible_orders = ["orders_clean.csv", "raw_orders_clean.csv"]
    orders_path = None
    for filename in possible_orders:
        test_path = os.path.join(silver_folder, filename)
        if os.path.exists(test_path):
            orders_path = test_path
            break

    if not orders_path:
        logger.error(f"Orders file not found. Tried: {possible_orders}")
        raise FileNotFoundError(f"Orders file not found in {silver_folder}")

    tickets_path = os.path.join(silver_folder, "support_tickets_clean.csv")

    orders_df = pd.read_csv(orders_path)
    tickets_df = pd.read_csv(tickets_path)
    
    # Count tickets per order
    if 'order_id' in tickets_df.columns:
        # Determine which column to use for issue types (issue_type or category)
        issue_col = 'issue_type' if 'issue_type' in tickets_df.columns else 'category' if 'category' in tickets_df.columns else None

        if issue_col:
            tickets_count = tickets_df.groupby('order_id').agg({
                'ticket_id': 'count',
                issue_col: lambda x: ', '.join(x.unique()) if len(x) > 0 else 'None'
            }).reset_index()
            tickets_count.columns = ['order_id', 'num_tickets', 'issue_types']
        else:
            tickets_count = tickets_df.groupby('order_id').agg({
                'ticket_id': 'count'
            }).reset_index()
            tickets_count.columns = ['order_id', 'num_tickets']
            tickets_count['issue_types'] = 'None'
    else:
        tickets_count = pd.DataFrame(columns=['order_id', 'num_tickets', 'issue_types'])
    
    # Merge orders with ticket counts
    # Identify the order ID column in orders (could be 'id' or 'order_id')
    order_id_col = 'id' if 'id' in orders_df.columns else 'order_id'

    if order_id_col in orders_df.columns:
        summary = orders_df.merge(tickets_count, left_on=order_id_col, right_on='order_id', how='left')

        # Fill missing values
        summary['num_tickets'] = summary['num_tickets'].fillna(0).astype(int)
        summary['issue_types'] = summary['issue_types'].fillna('None')

        # Add derived columns
        summary['has_issues'] = summary['num_tickets'] > 0

        # Save to Gold layer
        os.makedirs(gold_folder, exist_ok=True)
        output_path = os.path.join(gold_folder, "restaurant_summary.csv")
        summary.to_csv(output_path, index=False)

        logger.info(f"✓ Restaurant summary saved: {len(summary)} orders")
        return output_path
    else:
        logger.error("order ID column not found in orders data")
        raise ValueError("order ID column not found in orders data")


@task(name="Create Ticket Analytics")
def create_ticket_analytics(silver_folder: str, gold_folder: str) -> str:
    """
    Create detailed ticket analytics by issue type and priority.
    
    Args:
        silver_folder: Path to Silver layer folder
        gold_folder: Path to Gold layer folder
        
    Returns:
        Path to ticket analytics file
    """
    logger = get_run_logger()
    logger.info("Creating ticket analytics...")
    
    # Read cleaned tickets data
    tickets_path = os.path.join(silver_folder, "support_tickets_clean.csv")
    
    if not os.path.exists(tickets_path):
        logger.warning(f"Tickets file not found: {tickets_path}")
        return None
    
    tickets_df = pd.read_csv(tickets_path)

    # Analytics by issue type or category
    issue_col = 'issue_type' if 'issue_type' in tickets_df.columns else 'category' if 'category' in tickets_df.columns else None

    if issue_col:
        issue_analytics = tickets_df.groupby(issue_col).agg({
            'ticket_id': 'count'
        }).reset_index()
        issue_analytics.columns = ['issue_type', 'ticket_count']
        issue_analytics = issue_analytics.sort_values('ticket_count', ascending=False)

        # Add percentage
        issue_analytics['percentage'] = (
            issue_analytics['ticket_count'] / issue_analytics['ticket_count'].sum() * 100
        ).round(2)

        # Save to Gold layer
        os.makedirs(gold_folder, exist_ok=True)
        output_path = os.path.join(gold_folder, "ticket_analytics.csv")
        issue_analytics.to_csv(output_path, index=False)

        logger.info(f"✓ Ticket analytics saved: {len(issue_analytics)} issue types")
        return output_path
    else:
        logger.warning("issue_type or category column not found in tickets data")
        return None


@flow(name="Analytics Flow - Gold Layer")
def analytics_flow(
    silver_folder: str = "data/silver",
    gold_folder: str = "data/gold"
) -> dict:
    """
    Main analytics flow to populate Gold layer with metrics and insights.
    
    Args:
        silver_folder: Path to Silver layer folder
        gold_folder: Path to Gold layer folder
        
    Returns:
        Dictionary with analytics summary
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STARTING ANALYTICS FLOW - GOLD LAYER")
    logger.info("=" * 60)
    
    # Calculate all analytics
    aov_path = calculate_aov(silver_folder, gold_folder)
    tickets_per_order_path = calculate_tickets_per_order(silver_folder, gold_folder)
    summary_path = create_restaurant_summary(silver_folder, gold_folder)
    ticket_analytics_path = create_ticket_analytics(silver_folder, gold_folder)
    
    # Summary
    summary = {
        "aov": aov_path,
        "tickets_per_order": tickets_per_order_path,
        "restaurant_summary": summary_path,
        "ticket_analytics": ticket_analytics_path,
        "gold_folder": gold_folder
    }
    
    logger.info("=" * 60)
    logger.info("ANALYTICS COMPLETE")
    logger.info(f"Analytics outputs saved to: {gold_folder}")
    logger.info("=" * 60)
    
    return summary


if __name__ == "__main__":
    # Run the flow
    result = analytics_flow()

