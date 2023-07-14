# Databricks notebook source
# MAGIC %fs head '/FileStore/tables/Multisep.csv'

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/Multisep.csv',header=True)
df.show()

# COMMAND ----------

# DBTITLE 1,Splitting the into multiple columns
from pyspark.sql.functions import split
df1=df.withColumn("Physics",split(df.Marks,'\\|')[0]).withColumn("Chemistry",split(df.Marks,'\\|')[1]).withColumn("Math",split(df.Marks,'\\|')[2]).drop(df.Marks)
df1.show()

# COMMAND ----------

# DBTITLE 1,Splitting the into multiple rows
from pyspark.sql.functions import col,explode
df2= df.select("ID","NAME","Age",explode(split(df.Marks,'\\|')).alias("Marks"))
df2.show()

# COMMAND ----------

