SELECT
  d.date,
  f.channel,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.quantity) AS units,
  SUM(f.recognized_revenue) AS revenue
FROM retail_dev.gold.fact_sales f
JOIN retail_dev.gold.dim_date d ON f.date_sk = d.date_sk
GROUP BY d.date, f.channel
ORDER BY d.date, f.channel;

