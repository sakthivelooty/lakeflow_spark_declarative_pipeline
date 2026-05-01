
from pyspark import pipelines as dp
from pyspark.sql import functions as F
# Create_sauto_cdc_flow
# Databricks Documents
# https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes

# Step1: Create a empty streaming table 
dp.create_streaming_table("sdp_tutorial.silver.fact_sales")

dp.create_auto_cdc_flow(
  target = "sdp_tutorial.silver.fact_sales",
  source = "sdp_tutorial.bronze.valid_sales",
  keys = ["sales_id","customer_id","product_id"],
  sequence_by = "sale_timestamp",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)