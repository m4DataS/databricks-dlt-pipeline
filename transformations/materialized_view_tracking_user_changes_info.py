# from pyspark import pipelines as dp
# from pyspark.sql.functions import *

# @dp.table(
#   name = "customers_history_agg",
#   comment = "Aggregated customer history"
# )
# def customers_history_agg():
#   return (
#     spark.read.table("customers_history")
#       .groupBy("id")
#       .agg(
#           count("address").alias("address_count"),
#           count("email").alias("email_count"),
#           count("firstname").alias("firstname_count"),
#           count("lastname").alias("lastname_count")
#       )
#   )

from pyspark import pipelines as dp
from utilities.materialize_2_aggregates import compute_customer_history

@dp.table(name="2_customers_history_agg")
def customers_history_agg():
    return compute_customer_history()