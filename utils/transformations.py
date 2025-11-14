"""
Data transformation and cleaning utilities.
"""
import pandas as pd
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names: lowercase, replace spaces with underscores.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with cleaned column names
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
    return df


def handle_missing_values(df: pd.DataFrame, strategy: Dict[str, any] = None) -> pd.DataFrame:
    """
    Handle missing values based on specified strategy.
    
    Args:
        df: Input DataFrame
        strategy: Dictionary mapping column names to fill strategies
                 (e.g., {'column': 'mean', 'another': 0})
        
    Returns:
        DataFrame with missing values handled
    """
    df = df.copy()
    
    if strategy is None:
        # Default strategy: fill numeric with 0, categorical with 'Unknown'
        numeric_cols = df.select_dtypes(include=['number']).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        df[numeric_cols] = df[numeric_cols].fillna(0)
        df[categorical_cols] = df[categorical_cols].fillna('Unknown')
    else:
        for col, fill_value in strategy.items():
            if col in df.columns:
                if fill_value == 'mean':
                    df[col] = df[col].fillna(df[col].mean())
                elif fill_value == 'median':
                    df[col] = df[col].fillna(df[col].median())
                elif fill_value == 'mode':
                    df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown')
                else:
                    df[col] = df[col].fillna(fill_value)
    
    return df


def standardize_date_columns(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    """
    Convert specified columns to datetime format.
    
    Args:
        df: Input DataFrame
        date_columns: List of column names to convert to datetime
        
    Returns:
        DataFrame with standardized date columns
    """
    df = df.copy()
    
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"Converted {col} to datetime")
            except Exception as e:
                logger.warning(f"Could not convert {col} to datetime: {e}")
    
    return df


def remove_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Remove duplicate rows based on specified columns.
    
    Args:
        df: Input DataFrame
        subset: List of columns to consider for duplicates (None = all columns)
        
    Returns:
        DataFrame with duplicates removed
    """
    df = df.copy()
    initial_count = len(df)
    
    df = df.drop_duplicates(subset=subset, keep='first')
    
    removed_count = initial_count - len(df)
    if removed_count > 0:
        logger.info(f"Removed {removed_count} duplicate rows")
    
    return df


def merge_datasets(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    on: str,
    how: str = 'left'
) -> pd.DataFrame:
    """
    Merge two datasets on a common key.
    
    Args:
        left_df: Left DataFrame
        right_df: Right DataFrame
        on: Column name to join on
        how: Type of join ('left', 'right', 'inner', 'outer')
        
    Returns:
        Merged DataFrame
    """
    logger.info(f"Merging datasets on '{on}' using '{how}' join")
    logger.info(f"Left dataset shape: {left_df.shape}, Right dataset shape: {right_df.shape}")
    
    merged_df = pd.merge(left_df, right_df, on=on, how=how)
    
    logger.info(f"Merged dataset shape: {merged_df.shape}")
    
    return merged_df


def validate_data_quality(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that DataFrame contains required columns and has data.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if validation passes, False otherwise
    """
    if df.empty:
        logger.error("DataFrame is empty")
        return False
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    logger.info(f"Data quality validation passed. Shape: {df.shape}")
    return True


def clean_orders_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize orders data.
    
    Args:
        df: Raw orders DataFrame
        
    Returns:
        Cleaned orders DataFrame
    """
    df = df.copy()
    
    # Clean column names
    df = clean_column_names(df)
    
    # Standardize common column names
    column_mapping = {
        'orderid': 'order_id',
        'customerid': 'customer_id',
        'order_date': 'order_date',
        'total': 'total_value',
        'amount': 'total_value',
        'order_total': 'total_value'
    }
    
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Convert date columns
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    df = standardize_date_columns(df, date_cols)
    
    # Remove duplicates
    if 'order_id' in df.columns:
        df = remove_duplicates(df, subset=['order_id'])
    
    return df


def clean_tickets_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize support tickets data.
    
    Args:
        df: Raw tickets DataFrame
        
    Returns:
        Cleaned tickets DataFrame
    """
    df = df.copy()
    
    # Clean column names
    df = clean_column_names(df)
    
    # Standardize common column names
    column_mapping = {
        'ticketid': 'ticket_id',
        'orderid': 'order_id',
        'ticket_date': 'ticket_date',
        'created_at': 'ticket_date',
        'issue': 'issue_type',
        'status': 'ticket_status'
    }
    
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Convert date columns
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    df = standardize_date_columns(df, date_cols)
    
    # Remove duplicates
    if 'ticket_id' in df.columns:
        df = remove_duplicates(df, subset=['ticket_id'])
    
    return df

