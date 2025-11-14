# Bronze Layer - Raw Data Ingestion

## Overview
The Bronze layer is the first stage of our data pipeline. It handles raw data ingestion from multiple sources without any transformations. Think of it as our "landing zone" where data arrives in its original format.

## Data Sources

### 1. CSV Files (Local)
We ingest six CSV files from the `data/source_csv/` folder:

| File | Description | Approx. Rows |
|------|-------------|--------------|
| `raw_customers.csv` | Customer master data | 930 |
| `raw_orders.csv` | Order transactions | 63,148 |
| `raw_products.csv` | Product catalog | 10 |
| `raw_items.csv` | Order line items | 90,183 |
| `raw_stores.csv` | Store locations | 6 |
| `raw_supplies.csv` | Supply inventory | 65 |

### 2. Azure Blob Storage (Cloud)
We download support ticket data in JSONL format:

- **Source**: Azure Blob Storage container
- **Format**: JSONL (JSON Lines - one JSON object per line)
- **File**: `support_tickets.jsonl`
- **Records**: ~500,000 support tickets
- **Authentication**: SAS token (configured in `.env` file)

## What Happens in Bronze Layer?

The Bronze layer performs these operations:

1. **CSV Ingestion**
   - Reads all CSV files from `data/source_csv/`
   - Saves them as-is to `data/bronze/` with same filenames
   - No data cleaning or transformation

2. **Azure JSONL Download**
   - Connects to Azure Blob Storage using SAS token
   - Downloads `support_tickets.jsonl` file
   - Saves to `data/bronze/support_tickets.jsonl`
   - Handles large files efficiently (streaming download)

3. **Data Validation**
   - Checks if files exist and are readable
   - Logs row counts for each file
   - Reports any download or read errors

## Output Files

After Bronze layer execution, you'll find these files in `data/bronze/`:

```
data/bronze/
├── raw_customers.csv
├── raw_orders.csv
├── raw_products.csv
├── raw_items.csv
├── raw_stores.csv
├── raw_supplies.csv
└── support_tickets.jsonl
```

## Key Characteristics

- **No Transformations**: Data is stored exactly as received
- **Full History**: All records are preserved, including duplicates
- **Schema Flexibility**: No schema enforcement at this stage
- **Audit Trail**: Original data is always available for reference

## Prefect Tasks

The Bronze layer uses these Prefect tasks (defined in `flows/ingest_data.py`):

- `ingest_csv_files()` - Reads and saves CSV files
- `download_azure_jsonl()` - Downloads JSONL from Azure Blob Storage
- `ingest_data_flow()` - Orchestrates both tasks

## Troubleshooting

**Issue**: Azure download fails
- **Solution**: Check your `.env` file has correct `AZURE_BLOB_URL` and `AZURE_SAS_TOKEN`
- **Solution**: Verify SAS token hasn't expired

**Issue**: CSV files not found
- **Solution**: Ensure CSV files are in `data/source_csv/` folder
- **Solution**: Check file names match exactly (case-sensitive)

**Issue**: Permission errors
- **Solution**: Ensure `data/bronze/` folder exists and is writable
- **Solution**: Run `mkdir -p data/bronze` to create the folder

## Next Step

Once Bronze layer completes, data moves to the **Silver Layer** for cleaning and transformation.

See [Silver Layer Documentation](../silver/README.md) for details.

