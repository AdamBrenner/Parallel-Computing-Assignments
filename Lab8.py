# Databricks notebook source
# Adam Brenner
# Lab 8

sc = spark.sparkContext

# COMMAND ----------

# Create directory for Lab 8
# dbutils.fs.mkdirs("dbfs:///FileStore/tables/lab8Master")

# # Uncomment the following lines if you have not moved the 2 csv files required
# dbutils.fs.mv("dbfs:///FileStore/tables/FIFA.csv", "dbfs:///FileStore/tables/lab8Master")

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, LongType, TimestampType

fifaSchema = StructType( \
 [StructField('ID', LongType(), True), \
 StructField('lang', StringType(), True), \
 StructField('Date', TimestampType(), True), \
 StructField('Source', StringType(), True), \
 StructField('len', LongType(), True), \
 StructField('Orig_Tweet', StringType(), True), \
 StructField('Tweet', StringType(), True), \
 StructField('Likes', LongType(), True), \
 StructField('RTs', LongType(), True), \
 StructField('Hashtags', StringType(), True), \
 StructField('UserMentionNames', StringType(), True), \
 StructField('UserMentionID', StringType(), True), \
 StructField('Name', StringType(), True), \
 StructField('Place', StringType(), True), \
 StructField('Followers', LongType(), True), \
 StructField('Friends', LongType(), True), \
 ])


# COMMAND ----------

fifaDF = spark.read.format("csv").option("header", True).option("mode", "dropMalformed").schema(fifaSchema).load("dbfs:///FileStore/tables/lab8Master/FIFA.csv")

# COMMAND ----------

import pyspark.sql.functions as f
fifaDF.show()
fifaDF.createOrReplaceTempView("fifa")

# COMMAND ----------

fifaDF = fifaDF.orderBy('Date')
fifaDF = fifaDF.repartition(20)

# COMMAND ----------

fifaDF = spark.sql("SELECT * FROM fifa WHERE Hashtags IS NOT NULL")
fifaDF = fifaDF.select(f.col('ID'), f.col('Date'), f.col('Hashtags'))

# COMMAND ----------

from pyspark.sql import Row

df = fifaDF.withColumn('Hashtags', f.explode(f.split('Hashtags', ',')))
df.show()


# COMMAND ----------

from pyspark.sql.window import Window

window_length = 60
sliding_rate = 30

df2 = df.groupBy(f.window(f.col('Date'), "60 minutes", "30 minutes")).agg(f.count('Hashtags')).filter(f.count('Hashtags') > 100)
df2.show()

# COMMAND ----------

dbutils.fs.rm("FileStore/tables/trending", True)

df.write.format("csv").option("header", True).save("FileStore/tables/trending/")

# COMMAND ----------

fifaSchema2 = StructType( \
 [StructField('ID', LongType(), True), \
 StructField('Date', TimestampType(), True), \
 StructField('Hashtags', StringType(), True), \
 ])

sourceStream = spark.readStream.format("csv").option("header", True).schema(fifaSchema2).option("maxFilesPerTrigger", 1).load("dbfs:///FileStore/tables/trending")

trendHash = sourceStream.groupBy(f.window(f.col('Date'), "60 minutes", "30 minutes")).agg(f.count('Hashtags')).filter(f.count('Hashtags') > 100)

sinkStream = trendHash.writeStream.outputMode("complete").format("memory").queryName("TrendingHashtags").trigger(processingTime = '20 seconds').start()

# COMMAND ----------


