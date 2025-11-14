# Silver Layer - Data Cleaning & Transformation

## Overview
The Silver layer takes raw data from Bronze and applies cleaning, validation, and standardization. This is where messy data becomes reliable, analysis-ready data.

## What Happens in Silver Layer?

### 1. Data Cleaning
We apply these cleaning operations to all datasets:

- **Remove Duplicates**: Identify and remove duplicate records
- **Handle Missing Values**: 
  - Fill missing numeric values with 0 or median
  - Fill missing text values with "Unknown" or empty string
  - Drop rows with critical missing values (like IDs)
- **Standardize Formats**:
  - Convert date strings to datetime objects
  - Normalize text (trim whitespace, fix casing)
  - Ensure consistent data types

### 2. Column Normalization
- Rename columns to snake_case format
- Remove special characters from column names
- Ensure consistent naming across datasets

### 3. Data Type Conversions
- Convert string dates to datetime
- Ensure numeric columns are float/int (not strings)
- Convert boolean-like strings to actual booleans

### 4. Data Validation
- Check for negative values in price/amount columns
- Validate date ranges (no future dates for historical data)
- Ensure foreign key references exist

## Input Files (from Bronze)

The Silver layer reads these files from `data/bronze/`:

- `raw_customers.csv`
- `raw_orders.csv`
- `raw_products.csv`
- `raw_items.csv`
- `raw_stores.csv`
- `raw_supplies.csv`
- `support_tickets.jsonl`

## Output Files

After Silver layer execution, you'll find cleaned files in `data/silver/`:

```
data/silver/
├── raw_customers_clean.csv
├── raw_orders_clean.csv
├── raw_products_clean.csv
├── raw_items_clean.csv
├── raw_stores_clean.csv
├── raw_supplies_clean.csv
└── support_tickets_clean.csv
```

Note: JSONL file is converted to CSV for easier analysis.

## Transformation Details

### Customers
- Remove duplicate customer IDs
- Standardize customer names (title case)
- Fill missing names with "Unknown Customer"

### Orders
- Convert `ordered_at` to datetime
- Ensure `order_total`, `subtotal`, `tax_paid` are numeric
- Remove orders with missing customer references
- Validate order totals = subtotal + tax

### Products
- Standardize SKU format (uppercase)
- Ensure prices are positive numbers
- Fill missing descriptions with product name

### Items
- Validate order_id and SKU references exist
- Remove orphaned items (no matching order/product)
- Ensure quantity values are positive integers

### Stores
- Convert `opened_at` to datetime
- Validate tax_rate is between 0 and 1
- Standardize store names

### Supplies
- Standardize SKU references
- Ensure cost values are positive
- Convert perishable flag to boolean

### Support Tickets
- Parse JSONL to structured CSV
- Extract nested JSON fields
- Standardize issue_type categories
- Convert timestamps to datetime
- Link tickets to orders via order_id

## Data Quality Metrics

After Silver layer processing, we track:

- **Duplicate Removal Rate**: % of duplicates removed
- **Missing Value Fill Rate**: % of missing values handled
- **Data Type Conversion Success**: % of successful conversions
- **Validation Pass Rate**: % of records passing validation

These metrics are logged during pipeline execution.

## Prefect Tasks

The Silver layer uses these Prefect tasks (defined in `flows/transform_data.py`):

- `clean_csv_data()` - Cleans individual CSV files
- `clean_jsonl_data()` - Cleans and converts JSONL to CSV
- `transform_data_flow()` - Orchestrates all cleaning tasks

## Troubleshooting

**Issue**: High duplicate count
- **Cause**: Source data may have duplicate entries
- **Impact**: Duplicates are removed, only first occurrence kept

**Issue**: Many missing values
- **Cause**: Incomplete source data
- **Impact**: Missing values filled with defaults (may affect analytics)

**Issue**: Data type conversion errors
- **Cause**: Unexpected data formats in source
- **Solution**: Check Bronze layer files for data quality issues

**Issue**: Validation failures
- **Cause**: Data doesn't meet business rules
- **Solution**: Review validation logic in `utils/transformations.py`

## Next Step

Once Silver layer completes, clean data moves to the **Gold Layer** for analytics and insights.

See [Gold Layer Documentation](../gold/README.md) for details.

