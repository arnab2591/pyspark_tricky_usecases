# Databricks notebook source
SimpleData1 = [(1,["Arnab","Nilima"]),(2,["Sagar","Prajapati"]),(3,["Shivam","Gupta"]),(4,["Kim"])]

columns=["ID","Name"]

df1 = spark.createDataFrame(data = SimpleData1,schema=columns)
df1.show()

# COMMAND ----------

from pyspark.sql.functions import col,explode

df_output = df1.select(col("ID"),explode(col("Name")).alias("Name"))
df_output.show()

# COMMAND ----------

