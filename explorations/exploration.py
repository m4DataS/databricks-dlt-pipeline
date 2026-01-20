# Databricks notebook source
# Update these to match the catalog and schema
# that you used for the pipeline in step 1.
catalog = "dev_data_cloud_cicd"
schema = dbName = db = "demo_da_cicd"

display(spark.read.json(f"/Volumes/{catalog}/{schema}/raw_data/customers"))