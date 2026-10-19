SELECT
  p.category,
  p.brand,
  p.product_name,
  SUM(f.quantity) AS units,
  SUM(f.recognized_revenue) AS revenue,
  SUM(f.gross_amount - (p.cost_price * f.quantity)) AS estimated_margin
FROM retail_dev.gold.fact_sales f
JOIN retail_dev.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.category, p.brand, p.product_name
ORDER BY revenue DESC;

