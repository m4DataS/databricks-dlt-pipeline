# utilities/bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.getActiveSession()

def load_raw_customers(path: str):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(path)
    )