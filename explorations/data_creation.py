# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC # Load fake data in a volume to follow tuto

# COMMAND ----------

# Update these to match the catalog and schema
# that you used for the pipeline in step 1.
catalog = "dev_data_cloud_cicd"
schema = dbName = db = "demo_da_cicd"

spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{db}`.`raw_data`')
volume_folder =  f"/Volumes/{catalog}/{db}/raw_data"

try:
  dbutils.fs.ls(volume_folder+"/customers")
except:
  print(f"folder doesn't exist, generating the data under {volume_folder}...")

  from pyspark.sql import functions as F

  df = spark.range(0, 100000).repartition(100)

  df = (
      df
      .withColumn("id", F.expr("uuid()"))
      .withColumn("firstname", F.expr("element_at(array('John','Paul','Mike','Anna','Laura','Sarah'), int(rand()*6)+1)"))
      .withColumn("lastname", F.expr("element_at(array('Smith','Dupont','Martin','Bernard','Durand'), int(rand()*5)+1)"))
      .withColumn("email", F.expr("concat(lower(firstname), '.', lower(lastname), '@company.com')"))
      .withColumn("address", F.expr("concat(int(rand()*999), ' Main Street')"))
      .withColumn("operation", F.expr("element_at(array('APPEND','UPDATE','DELETE'), int(rand()*3)+1)"))
      .withColumn("operation_date", F.current_timestamp())
  )

  df.repartition(100).write.format("json").mode("overwrite").save(volume_folder + "/customers")
  # print(f"folder doesn't exist, generating the data under {volume_folder}...")
  # from pyspark.sql import functions as F
  # from faker import Faker
  # from collections import OrderedDict
  # import uuid
  # fake = Faker()
  # import random

  # fake_firstname = F.udf(fake.first_name)
  # fake_lastname = F.udf(fake.last_name)
  # fake_email = F.udf(fake.ascii_company_email)
  # fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
  # fake_address = F.udf(fake.address)
  # operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
  # fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
  # fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)

  # df = spark.range(0, 100000).repartition(100)
  # df = df.withColumn("id", fake_id())
  # df = df.withColumn("firstname", fake_firstname())
  # df = df.withColumn("lastname", fake_lastname())
  # df = df.withColumn("email", fake_email())
  # df = df.withColumn("address", fake_address())
  # df = df.withColumn("operation", fake_operation())
  # df_customers = df.withColumn("operation_date", fake_date())
  # df_customers.repartition(100).write.format("json").mode("overwrite").save(volume_folder+"/customers")