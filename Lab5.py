# Databricks notebook source
# Adam Brenner

sc = spark.sparkContext

# COMMAND ----------

# Create directory for the baseball master.csv
dbutils.fs.mkdirs("dbfs:///FileStore/tables/lab5Master")

# Uncomment the following line if you haven't moved the master.csv to the lab5 folder
# dbutils.fs.mv("dbfs:///FileStore/tables/Master_1_.csv", "dbfs:///FileStore/tables/lab5Master")

# COMMAND ----------

from itertools import islice
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import Row

# Read in the csv file as a text file
file = sc.textFile("dbfs:///FileStore/tables/lab5Master/Master_1_.csv")

# Split the elements and map the rows that we want, in this case playerID, birthCountry, birthState, and height
master = file.map(lambda l: [l.split(",")[0], l.split(",")[4], l.split(",")[5], l.split(",")[17]])

# Ignore the header
master = master.mapPartitionsWithIndex(lambda idx, i: islice(i, 1, None) if idx == 0 else i)

# Filtering any rows that have a height of non-existent
master = master.filter(lambda x: x[3] != '')

# Mapping the elements to their respective columns
masterRows = master.map(lambda r: Row(playerID = r[0], birthCountry = r[1], birthState = r[2], height = int(r[3])))

# The following code creates the dataframe without using a schema
# masterDF = spark.createDataFrame(masterRows)
# masterDF.printSchema()
# masterDF.show()

# Schema used for creating the data frame
masterSchema = StructType( [ \
                             StructField('playerID', StringType(), True), \
                             StructField('birthCountry', StringType(), True), \
                             StructField('birthState', StringType(), True), \
                             StructField('height', LongType(), True)
                             ])

baseMaster = spark.createDataFrame(masterRows, masterSchema)

# The Data Frame we created from the schema
baseMaster.show(30)

# COMMAND ----------

# Players born in Colorado

import pyspark.sql.functions

baseMaster.createOrReplaceTempView("master")

print("Number of Players born in Colorado SQL Query: ")
print(spark.sql("SELECT birthState FROM master WHERE birthState == 'CO'").count())

print("Number of Players born in Colorado DF Function: ")
print(baseMaster.filter(baseMaster["birthState"] == "CO").count())

# COMMAND ----------

# Average Player Height by Country

import pyspark.sql.functions

baseMaster.createOrReplaceTempView("master")

print("Average height of all players by birth country SQL query: ")
spark.sql("SELECT birthCountry, avg(height) FROM master GROUP BY birthCountry ORDER BY avg(height) DESC").show(baseMaster.count())

avgHeight = baseMaster.groupBy("birthCountry").avg("height").orderBy('avg(height)', ascending = False)
print("Average height of all players by birth country DF Function: ")
avgHeight.show(baseMaster.count())

# COMMAND ----------


