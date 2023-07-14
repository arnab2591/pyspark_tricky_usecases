# Databricks notebook source
# DBTITLE 1,DataFrame 1
SimpleData1 = [(1,"Arnab","CSE","WB",100),(2,"Shivam","IT","MP",86),(3,"Muni","Mech","AP",70)]

columns=["ID","Student_Name","Department_Name","City","Marks"]

df1 = spark.createDataFrame(data = SimpleData1,schema=columns)
df1.show()

# COMMAND ----------

SimpleData2 = [(5,"Raj","CSE","WB"),(7,"Kunal","IT","Rajasthan")]

columns=["ID","StudentName","Department","City"]

df2 = spark.createDataFrame(data = SimpleData2,schema=columns)
df2.show()

# COMMAND ----------

df=df1.union(df2)
df.show()

# COMMAND ----------

from pyspark.sql.functions import lit
df2=df2.withColumn("Marks",lit("null"))
df2.show()

# COMMAND ----------

df=df1.union(df2)
df.show()

# COMMAND ----------

#The PySpark SQL functions lit() are used to add a new column to the DataFrame by assigning a literal or constant value
# https://sparkbyexamples.com/pyspark/pyspark-lit-add-literal-constant/