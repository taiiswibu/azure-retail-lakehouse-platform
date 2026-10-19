-- Change this catalog name if you deploy with a non-default bundle variable.
USE CATALOG retail_dev;
USE SCHEMA gold;

CREATE OR REPLACE VIEW vw_sales_detail AS
SELECT
  f.sales_sk,
  f.order_id,
  f.order_item_id,
  f.order_ts,
  d.date,
  d.year,
  d.quarter,
  d.month,
  d.month_name,
  c.customer_id,
  c.full_name,
  c.city AS customer_city,
  c.loyalty_tier,
  c.acquisition_source,
  p.product_id,
  p.sku,
  p.product_name,
  p.category,
  p.brand,
  f.channel,
  f.status,
  f.quantity,
  f.gross_amount,
  f.discount_amount,
  f.net_amount,
  f.recognized_revenue
FROM fact_sales f
LEFT JOIN dim_date d ON f.date_sk = d.date_sk
LEFT JOIN dim_customer c ON f.customer_sk = c.customer_sk
LEFT JOIN dim_product p ON f.product_sk = p.product_sk;

CREATE OR REPLACE VIEW vw_executive_kpis AS
SELECT
  SUM(recognized_revenue) AS total_revenue,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_sk) AS buying_customers,
  SUM(quantity) AS units_sold,
  SUM(recognized_revenue) / NULLIF(COUNT(DISTINCT order_id), 0) AS average_order_value,
  SUM(CASE WHEN status IN ('cancelled', 'refunded') THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0) AS cancellation_refund_item_rate
FROM fact_sales;

CREATE OR REPLACE VIEW vw_data_quality_latest AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY table_name, check_name ORDER BY checked_at DESC) AS row_num
  FROM retail_dev.ops.data_quality_results
)
SELECT run_id, table_name, check_name, status, observed_value, threshold, details, checked_at
FROM ranked
WHERE row_num = 1;

