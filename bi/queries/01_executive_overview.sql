SELECT
  SUM(recognized_revenue) AS total_revenue,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_sk) AS buying_customers,
  SUM(quantity) AS units_sold,
  SUM(recognized_revenue) / NULLIF(COUNT(DISTINCT order_id), 0) AS average_order_value
FROM retail_dev.gold.fact_sales;

