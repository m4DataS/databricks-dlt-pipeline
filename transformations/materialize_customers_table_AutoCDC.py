# from pyspark import pipelines as dp
# from pyspark.sql.functions import *

# dp.create_streaming_table(name="customers_dlt", comment="Clean, materialized customers")

# dp.create_auto_cdc_flow(
#   target="customers_dlt",  # The customer table being materialized
#   source="customers_cdc_clean",  # the incoming CDC
#   keys=["id"],  # what we'll be using to match the rows to upsert
#   sequence_by=col("operation_date"),  # de-duplicate by operation date, getting the most recent value
#   ignore_null_updates=False,
#   apply_as_deletes=expr("operation = 'DELETE'"),  # DELETE condition
#   except_column_list=["operation", "operation_date", "_rescued_data"],
# )

from pyspark import pipelines as dp
from utilities.materialize_1 import cdc_materialization_params

dp.create_streaming_table(name="customers_dlt", comment="Clean, materialized customers")

dp.create_auto_cdc_flow(
    target="customers_dlt",
    source="customers_cdc_clean",
    **cdc_materialization_params()
)