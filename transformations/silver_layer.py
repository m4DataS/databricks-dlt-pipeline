# from pyspark import pipelines as dp
# from pyspark.sql.functions import *

# dp.create_streaming_table(
#   name = "customers_cdc_clean",
#   expect_all_or_drop = {"no_rescued_data": "_rescued_data IS NULL","valid_id": "id IS NOT NULL","valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"}
#   )

# @dp.append_flow(
#   target = "customers_cdc_clean",
#   name = "customers_cdc_clean_flow"
# )
# def customers_cdc_clean_flow():
#   return (
#       spark.readStream.table("customers_cdc_bronze")
#           .select("address", "email", "id", "firstname", "lastname", "operation", "operation_date", "_rescued_data")
#   )

from pyspark import pipelines as dp
from utilities.silver import load_and_clean_bronze

dp.create_streaming_table(
    name="customers_cdc_clean",
    expect_all_or_drop={
        "no_rescued_data": "_rescued_data IS NULL",
        "valid_id": "id IS NOT NULL",
        "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
    }
)

@dp.append_flow(target="customers_cdc_clean")
def customers_cdc_clean_flow():
    return load_and_clean_bronze()