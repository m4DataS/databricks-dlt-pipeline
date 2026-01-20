from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.getActiveSession()

def clean_bronze(df):
    return df.select(
        "address", "email", "id", "firstname", "lastname",
        "operation", "operation_date", "_rescued_data"
    )

def load_and_clean_bronze():
    df = spark.readStream.table("customers_cdc_bronze")
    return clean_bronze(df)