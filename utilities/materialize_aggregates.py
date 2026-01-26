from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.getActiveSession()

def compute_customer_history():
    return (
        spark.read.table("customers_history")
            .groupBy("id")
            .agg(
                count("address").alias("address_count"),
                count("email").alias("email_count"),
                count("firstname").alias("firstname_count"),
                count("lastname").alias("lastname_count")
            )
    )