# Databricks notebook source
# MAGIC %fs head '/FileStore/tables/Null_count.csv'

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/Null_count.csv',header=True)
df.show()

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.functions import count,col,when
df2 = spark.read.option("nullvalue","null").csv('/FileStore/tables/Null_count.csv',header=True)
df2.show()
df_2 =df2.select([count(when(col(i).isNull(),i)).alias(i) for i in df2.columns])
df_2.show()

# COMMAND ----------

from pyspark.sql.functions import count,col
df_1 =df.select([count(col(i).isNull()) for i in df.columns])
df_1.show()

# COMMAND ----------

