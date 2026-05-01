from pyspark import pipelines as dp
# Sink
# https://docs.databricks.com/aws/en/ldp/ldp-sinks


dp.create_sink(
    name = 'region_business_sales',
    format = 'delta',
    options={"tableName": "workspace.default.region_business_sales"}
)

@dp.append_flow(target = 'region_business_sales')
def region_business_sales():
  return spark.readStream.table('sdp_tutorial.gold.business_sales')


@dp.materialized_view(
  name = 'sdp_tutorial.gold.users_python'
)
def users_python():
  return spark.read.table('sdp_tutorial.silver.users')