"""
Azure Blob Storage utilities for reading JSONL data.
"""
import json
import logging
from typing import List, Dict, Any
from azure.storage.blob import ContainerClient
import pandas as pd

logger = logging.getLogger(__name__)


def parse_sas_url(blob_url: str) -> tuple[str, str]:
    """
    Parse Azure Blob URL with SAS token into base URL and SAS token.
    
    Args:
        blob_url: Full Azure Blob URL with SAS token
        
    Returns:
        Tuple of (base_url, sas_token)
    """
    if '?' in blob_url:
        base_url, sas_token = blob_url.split('?', 1)
        return base_url, sas_token
    return blob_url, ""


def read_jsonl_from_azure(blob_url: str, sas_token: str = None) -> pd.DataFrame:
    """
    Read JSONL files from Azure Blob Storage and return as DataFrame.
    
    Args:
        blob_url: Azure Blob Storage container URL
        sas_token: SAS token for authentication (optional if included in URL)
        
    Returns:
        DataFrame containing all JSONL records
    """
    try:
        # Parse URL if SAS token is embedded
        if sas_token is None and '?' in blob_url:
            blob_url, sas_token = parse_sas_url(blob_url)
        
        # Create container client with SAS token
        if sas_token:
            container_url = f"{blob_url}?{sas_token}"
        else:
            container_url = blob_url
            
        logger.info(f"Connecting to Azure Blob Storage container...")
        container_client = ContainerClient.from_container_url(container_url)
        
        # List all blobs in the container
        blob_list = container_client.list_blobs()
        
        all_records = []
        
        # Read each JSONL file
        for blob in blob_list:
            if blob.name.endswith('.jsonl'):
                logger.info(f"Reading blob: {blob.name}")
                blob_client = container_client.get_blob_client(blob.name)
                
                # Download blob content
                blob_data = blob_client.download_blob().readall()
                
                # Parse JSONL (each line is a JSON object)
                lines = blob_data.decode('utf-8').strip().split('\n')
                for line in lines:
                    if line.strip():  # Skip empty lines
                        try:
                            record = json.loads(line)
                            all_records.append(record)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse line: {line[:100]}... Error: {e}")
        
        logger.info(f"Successfully read {len(all_records)} records from Azure Blob Storage")
        
        # Convert to DataFrame
        if all_records:
            df = pd.DataFrame(all_records)
            return df
        else:
            logger.warning("No records found in Azure Blob Storage")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error reading from Azure Blob Storage: {e}")
        raise


def download_blob_to_file(blob_url: str, sas_token: str, output_path: str) -> None:
    """
    Download a specific blob to a local file.
    
    Args:
        blob_url: Full URL to the blob
        sas_token: SAS token for authentication
        output_path: Local path to save the file
    """
    try:
        if sas_token:
            full_url = f"{blob_url}?{sas_token}"
        else:
            full_url = blob_url
            
        container_client = ContainerClient.from_container_url(full_url)
        
        # Get the first blob (or specific blob if needed)
        blob_list = list(container_client.list_blobs())
        if blob_list:
            blob_client = container_client.get_blob_client(blob_list[0].name)
            
            with open(output_path, "wb") as f:
                blob_data = blob_client.download_blob()
                f.write(blob_data.readall())
                
            logger.info(f"Downloaded blob to {output_path}")
        else:
            logger.warning("No blobs found in container")
            
    except Exception as e:
        logger.error(f"Error downloading blob: {e}")
        raise

