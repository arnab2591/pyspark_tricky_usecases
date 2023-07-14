# Databricks notebook source
1-A-12-2-B-23-3-C-34-4-D-15

# COMMAND ----------

# dbutils.fs.mkdirs('/FileStore/tables/interview.txt')

# COMMAND ----------

dbutils.fs.put('/FileStore/tables/interview.txt','1-A-12-2-B-23-3-C-34-4-D-15')

# COMMAND ----------

df = spark.read.text('/FileStore/tables/interview.txt')
df.show()

# COMMAND ----------

# DBTITLE 1,Extracting data by pattern from a single row of data using regexp_replace command
from pyspark.sql.functions import regexp_replace
df1 = df.withColumn("new_value",regexp_replace("value","(.*?\\-){3}","$0,")).drop("value")
df1.show()

# COMMAND ----------

# DBTITLE 1,Single row into multiple row using explode command
from pyspark.sql.functions import explode,split
df2= df1.select(explode(split(df1.new_value,'-,')).alias("new_value"))
df2.show()

# COMMAND ----------

# DBTITLE 1,Splitting single column into multiple columns
df3 = df2.withColumn("ID",split(df2.new_value,'-')[0]).withColumn("Name",split(df2.new_value,'-')[1]).withColumn("Marks",split(df2.new_value,'-')[2]).drop(df2.new_value)
df3.show()

# COMMAND ----------

