# Databricks notebook source
# MAGIC %md
# MAGIC # Interoperability
# MAGIC Many customers need Databricks and Snowflake to work seamlessly together. We will be covering two different aspects of interoperability:
# MAGIC - **Snowflake** `reads` from Databricks
# MAGIC - **Databricks** `reads` from Snowflake*
# MAGIC
# MAGIC _Note: Databricks provides multiple ways to access data residing in Snowflake. Some options are best suited for ad-hoc or lightweight processing, some are better suited for strict governance requirements_

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("iceberg_catalog", "")
dbutils.widgets.text("iceberg_schema", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
iceberg_catalog = dbutils.widgets.get("iceberg_catalog")
iceberg_schema = dbutils.widgets.get("iceberg_schema")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Reads Snowflake Tables
# MAGIC <img src = "./setup/databricks_reads_snowflake_arch.png" width="800">
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create External Location for Iceberg Data
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `iceberg_external_loaction_theme_park`
# MAGIC URL 's3://themeparkiceberg'
# MAGIC WITH (CREDENTIAL databrics_iceberg_role) --you need to create a role in aws
# MAGIC COMMENT 'External location for iceberg data';

# COMMAND ----------

# DBTITLE 1,List External Data Storage Locations
# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Iceberg Tables in Snowflake
# MAGIC
# MAGIC For customers who are looking to have their Snowflake Iceberg tables availible in Databrick:
# MAGIC - Create an [External Iceberg Table](https://docs.snowflake.com/en/user-guide/tutorials/create-your-first-iceberg-table#create-an-external-volume) in Snowflake. The Volume in Snowflake should point to the location we designated above.
# MAGIC - Hydrate the table with data (eg CTAS, Insert, etc).
# MAGIC ![](./setup/create_iceberg_snowflake_worksheet.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Connection to Horizon Catalog
# MAGIC We will create a [catalog connection](https://e2-demo-field-eng.cloud.databricks.com/explore/locations?o=1444828305810485) similar to a foriegn database connection. However we will specify:
# MAGIC - The external location we created above
# MAGIC - An external location to store metadata

# COMMAND ----------

# DBTITLE 1,Convert PEM Private Key to Hex Format
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import re

with open("PATH/TO/YOUR/PRIVATE_KEY", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )
private_key_pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
).decode("utf-8")
private_key_hex64 = re.sub(r"-----.*-----|\n", "", private_key_pem)
# print(private_key_hex64)

# COMMAND ----------

# DBTITLE 1,Read Theme Park Visitor Data from Iceberg Table
display(spark.read.table(f'{iceberg_catalog}.{iceberg_schema}.theme_park_visitor_iceberg'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Reads Databricks Tables
# MAGIC Connect to Unity Catalog's Iceberg REST APIs from Snowflake to read a single source data file as Iceberg.
# MAGIC - Write a table to UC
# MAGIC - Generate Iceberg Metadata
# MAGIC - Enable Snowflake & Databricks Catalog Integration
# MAGIC <img src="./setup/snowflake_reads_databricks_arch.png" width="800">
# MAGIC
# MAGIC ### Documentation
# MAGIC We will be following this awesome blog, [here](https://www.databricks.com/blog/read-unity-catalog-tables-in-snowflake)!

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Configure Catalog and Schema Widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# DBTITLE 1,Create Iceberg Table
# MAGIC %sql
# MAGIC CREATE TABLE sb.theme_park.visitors_metric_view_snowflake_V2
# MAGIC as
# MAGIC SELECT
# MAGIC  date_format(`Month`, 'yyyy-MM') as `Month`,
# MAGIC  MEASURE(TotalVisits) as `Total_Visits`,
# MAGIC  MEASURE(TotalRevenue) as `Total_Revenue`,
# MAGIC  MEASURE(TotalSpendingPerCustomer) as `Avg_Spending_per_CX`
# MAGIC FROM sb.theme_park.visitors_metric_view -- hear is the metric view
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# DBTITLE 1,Purge Deletion Vectors that are enabled
# MAGIC %sql
# MAGIC -- iceberg v2 does not support deletion vectors, v3 does
# MAGIC REORG TABLE sb.theme_park.visitors_metric_view_snowflake_V2 APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=2));

# COMMAND ----------

# DBTITLE 1,Configure Delta Table Properties for Visitor Metrics
# MAGIC %sql
# MAGIC ALTER TABLE sb.theme_park.visitors_metric_view_snowflake_V2 SET TBLPROPERTIES(
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg');

# COMMAND ----------

# MAGIC %md
# MAGIC # Authentication
# MAGIC It is recommended to use a service principal for both development and production workloads.
# MAGIC
# MAGIC What you'll need:
# MAGIC - Create Service Principal in Workspace Admin Settings
# MAGIC - Save `client id` & `secret`

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Table in Snowflake
# MAGIC Snowflake Horizon catalog is able to connect to UC's Iceberg REST Catalog. After setting up the catalog integration, we will use Snowflake's [vended credentials](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-vended-credentials) for Iceberg to simplify access to underlying storage.
# MAGIC ![](./setup/snowflake_read_databricks_worksheet)