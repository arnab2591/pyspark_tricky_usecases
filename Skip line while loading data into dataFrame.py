# Databricks notebook source
df=spark.read.csv('/FileStore/tables/Skip_line.csv')
df.show()

# COMMAND ----------

# DBTITLE 1,Creating a rdd and processing it to get the end result
rdd = sc.textFile('/FileStore/tables/Skip_line.csv')
rdd.collect()

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Skip_line.csv').zipWithIndex()
rdd.collect()

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Skip_line.csv').zipWithIndex().filter(lambda x : x[1] > 3)
rdd.collect()

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Skip_line.csv').zipWithIndex().filter(lambda x : x[1] > 3).map(lambda x : x[0].split(','))
rdd.collect()

# COMMAND ----------

column_rdd = rdd.first()
column_rdd

# COMMAND ----------

main_rdd = rdd.filter(lambda a:a!=column_rdd)
main_rdd.collect()

# COMMAND ----------

df_final = main_rdd.toDF(column_rdd)
display(df_final)

# COMMAND ----------

df_final.show()

# COMMAND ----------

