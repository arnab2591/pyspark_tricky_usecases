# Databricks notebook source
SimpleData1 = [(1,"Arnab","U87653423y"),(2,"Shivam","8017650984"),(3,"Muni","97860546U2")]

columns=["ID","Student_Name","Mobile_no"]

df1 = spark.createDataFrame(data = SimpleData1,schema=columns)
df1.show()

# COMMAND ----------

# Create a view or table

temp_table_name = "phone_data"

df1.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

df = spark.sql("select * from phone_data")
df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df1.select("*").filter(col("Mobile_no").rlike("^[0-9]*$")).show()

# COMMAND ----------

spark.sql("select * from phone_data where Mobile_no rlike '^[0-9]*$'").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from phone_data where Mobile_no rlike "^[0-9]*$"

# COMMAND ----------

