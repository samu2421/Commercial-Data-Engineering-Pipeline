# Gold Layer - Analytics & Business Insights

## Overview
The Gold layer is where clean data becomes actionable insights. We compute business metrics, generate reports, and create aggregated views that answer key business questions.

## What Happens in Gold Layer?

The Gold layer performs these analytics:

1. **Aggregate Metrics**: Calculate KPIs like Average Order Value, Total Revenue
2. **Join Operations**: Combine data from multiple sources (orders + tickets + customers)
3. **Statistical Analysis**: Compute averages, counts, percentages
4. **Business Reports**: Generate summary tables for decision-making

## Input Files (from Silver)

The Gold layer reads cleaned files from `data/silver/`:

- `raw_orders_clean.csv`
- `raw_customers_clean.csv`
- `support_tickets_clean.csv`
- `raw_items_clean.csv`
- `raw_products_clean.csv`

## Output Files

After Gold layer execution, you'll find 5 analytics files in `data/gold/`:

### 1. `average_order_value.csv`
**Purpose**: Calculate Average Order Value (AOV) per customer

**Columns**:
- `customer`: Customer ID
- `total_orders`: Number of orders placed
- `total_spent`: Total amount spent
- `average_order_value`: AOV (total_spent / total_orders)

**Sample Data**:
```
customer,total_orders,total_spent,average_order_value
CUST001,15,15687.50,1045.83
CUST002,8,8234.00,1029.25
```

**Business Use**: Identify high-value customers, segment customers by spending

---

### 2. `tickets_per_order.csv`
**Purpose**: Analyze support ticket volume per order

**Columns**:
- `order_id`: Order ID
- `customer`: Customer ID
- `order_total`: Order amount
- `ticket_count`: Number of support tickets for this order
- `ordered_at`: Order date

**Sample Data**:
```
order_id,customer,order_total,ticket_count,ordered_at
ORD001,CUST001,1250.00,3,2024-01-15
ORD002,CUST002,890.50,1,2024-01-16
```

**Business Use**: Identify problematic orders, correlate order value with support needs

---

### 3. `restaurant_summary.csv`
**Purpose**: Comprehensive order and ticket summary

**Columns**:
- `order_id`: Order ID
- `customer`: Customer ID
- `order_total`: Order amount
- `ordered_at`: Order date
- `ticket_count`: Support tickets for this order
- `avg_order_value`: Customer's average order value

**Sample Data**:
```
order_id,customer,order_total,ordered_at,ticket_count,avg_order_value
ORD001,CUST001,1250.00,2024-01-15,3,1045.83
```

**Business Use**: Complete view of customer behavior, order patterns, support needs

---

### 4. `ticket_analytics.csv`
**Purpose**: Support ticket breakdown by category

**Columns**:
- `issue_type`: Ticket category (e.g., "Delivery Issue", "Product Quality")
- `ticket_count`: Number of tickets in this category
- `percentage`: Percentage of total tickets

**Sample Data**:
```
issue_type,ticket_count,percentage
Delivery Issue,125000,25.00
Product Quality,100000,20.00
Payment Problem,75000,15.00
```

**Business Use**: Identify top support issues, prioritize operational improvements

---

### 5. `overall_metrics.csv`
**Purpose**: High-level business KPIs

**Columns**:
- `metric`: Metric name
- `value`: Average Order Value
- `total_revenue`: Total revenue across all orders
- `total_orders`: Total number of orders

**Sample Data**:
```
metric,value,total_revenue,total_orders
overall,1045.77,66000000.00,63148
```

**Business Use**: Executive dashboard, performance tracking, trend analysis

---

## Analytics Logic

### Average Order Value (AOV)
```
AOV = Total Revenue / Total Orders
```

For per-customer AOV:
```
Customer AOV = Customer Total Spent / Customer Total Orders
```

### Ticket Coverage
```
Ticket Coverage = (Orders with Tickets / Total Orders) × 100
```

### Category Percentage
```
Category % = (Category Ticket Count / Total Tickets) × 100
```

## Prefect Tasks

The Gold layer uses these Prefect tasks (defined in `flows/analytics.py`):

- `calculate_aov()` - Computes average order value per customer
- `calculate_tickets_per_order()` - Joins orders with support tickets
- `create_restaurant_summary()` - Generates comprehensive summary
- `analyze_ticket_categories()` - Breaks down tickets by issue type
- `calculate_overall_metrics()` - Computes high-level KPIs
- `analytics_flow()` - Orchestrates all analytics tasks

## Key Insights from Current Data

Based on the latest pipeline run:

- **Average Order Value**: $1,045.77
- **Total Revenue**: $66M+
- **Total Orders**: 63,148
- **Total Support Tickets**: 500,000
- **Ticket Coverage**: 99.8% (almost all orders have support tickets)
- **Average Tickets per Order**: 6.34

**Top Support Issues**:
1. Delivery Issues (~25%)
2. Product Quality (~20%)
3. Payment Problems (~15%)

## Business Recommendations

Based on analytics:

1. **High Ticket Volume**: 6+ tickets per order suggests operational issues
2. **Delivery Focus**: 25% of tickets are delivery-related - improve logistics
3. **Quality Control**: 20% product quality issues - review suppliers
4. **Customer Segmentation**: Use AOV to create VIP customer programs

## Troubleshooting

**Issue**: Missing analytics files
- **Solution**: Check Silver layer completed successfully
- **Solution**: Verify Silver files exist in `data/silver/`

**Issue**: Zero or null values in metrics
- **Solution**: Check data quality in Silver layer
- **Solution**: Verify join keys (customer, order_id) are consistent

**Issue**: Unexpected metric values
- **Solution**: Review calculation logic in `flows/analytics.py`
- **Solution**: Check for data anomalies in Silver layer

## Next Steps

The Gold layer outputs are ready for:

- **Business Intelligence Tools**: Import CSVs into Tableau, Power BI
- **Reporting**: Generate executive reports and dashboards
- **Machine Learning**: Use as features for predictive models
- **API Integration**: Serve metrics via REST API

---

**Pipeline Complete!** Your data has traveled from raw sources (Bronze) → cleaned data (Silver) → actionable insights (Gold).

