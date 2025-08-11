# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC  date_format(`Month`, 'yyyy-MM') as `Month`,
# MAGIC  MEASURE(TotalVisits) as `Total_Visits`,
# MAGIC  MEASURE(TotalRevenue) as `Total_Revenue`,
# MAGIC  MEASURE(TotalSpendingPerCustomer) as `Avg_Spending_per_CX`
# MAGIC FROM sb.theme_park.visitors_metric_view -- here is the metric view
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED sb.theme_park.visitors_metric_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED sb.theme_park.theme_park_visitors;

# COMMAND ----------

