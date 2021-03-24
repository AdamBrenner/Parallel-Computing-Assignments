# Databricks notebook source
# Adam Brenner
# Lab 9

sc = spark.sparkContext

# COMMAND ----------

# # Create directory for Lab 9
# dbutils.fs.mkdirs("dbfs:///FileStore/tables/lab9Master")

# # Uncomment the following lines if you have not moved the 2 csv files required
# dbutils.fs.mv("dbfs:///FileStore/tables/airports.csv", "dbfs:///FileStore/tables/lab9Master")
# dbutils.fs.mv("dbfs:///FileStore/tables/routes.csv", "dbfs:///FileStore/tables/lab9Master")

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, LongType, FloatType

airportSchema = StructType( \
  [StructField('airportID', LongType(), True), \
  StructField('name', StringType(), True), \
  StructField('city', StringType(), True), \
  StructField('country', StringType(), True), \
  StructField('IATA', StringType(), True), \
  StructField('ICAO', StringType(), True), \
  StructField('Lat', FloatType(), True), \
  StructField('Long', FloatType(), True), \
  StructField('Alt', LongType(), True), \
  StructField('timeZone', FloatType(), True), \
  StructField('DST', StringType(), True), \
  StructField('databaseTimeZone', StringType(), True), \
  StructField('type', StringType(), True), \
  StructField('source', StringType(), True), \
  ])

routesSchema = StructType( \
  [StructField('airline', StringType(), True), \
  StructField('airlineID', LongType(), True), \
  StructField('sourceAirport', StringType(), True), \
  StructField('sourceAirportID', LongType(), True), \
  StructField('destinationAirport', StringType(), True), \
  StructField('destinationAirportID', LongType(), True), \
  StructField('codeshare', LongType(), True), \
  StructField('stops', LongType(), True), \
  StructField('equipment', StringType(), True), \
  ])

# COMMAND ----------

airportData = spark.read.format("csv").option("header", False).schema(airportSchema).load("dbfs:///FileStore/tables/lab9Master/airports.csv")
routesData = spark.read.format("csv").option("header", True).schema(routesSchema).load("dbfs:///FileStore/tables/lab9Master/routes.csv")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Keeping only certain columns of the airport data
airportDF = airportData[['airportID', 'country', 'IATA']]
airportDF.show()

# COMMAND ----------

# Keeping only certain columns of the routes data
routesDF = routesData[['sourceAirport', 'destinationAirport']]
routesDF.show()
routesDF.count()

# COMMAND ----------

routesDF = routesDF.drop_duplicates()

airRoutes = routesDF.join(airportDF, [routesDF.sourceAirport == airportDF.IATA])
airRoutes2 = routesDF.join(airportDF, [airRoutes.destinationAirport == airportDF.IATA])

airRoutes2 = airRoutes2.withColumn("src2", airRoutes2.sourceAirport). \
                        withColumn("dst2", airRoutes2.destinationAirport). \
                        withColumn("air2", airRoutes2.airportID). \
                        withColumn("IATA2", airRoutes2.IATA). \
                        withColumn("destCountry", airRoutes2.country). \
                        drop('sourceAirport', 'destinationAirport', 'airportID', 'IATA', 'country')

ar3 = airRoutes.join(airRoutes2, [airRoutes.sourceAirport == airRoutes2.src2, airRoutes.destinationAirport == airRoutes2.dst2])
ar3 = ar3.filter(col('country') == 'United States').filter(col('destCountry') == 'United States')
ar3 = ar3.drop('src2', 'dst2', 'air2', 'IATA2')

# COMMAND ----------

ar3.show()

# COMMAND ----------

edges = ar3[['sourceAirport', 'destinationAirport']].withColumn("src", ar3.sourceAirport).withColumn("dst", ar3.destinationAirport). \
                                                     drop("sourceAirport", "destinationAirport")

# COMMAND ----------

a = edges.select('src').distinct()
b = edges.select('dst').distinct()
ab = a.union(b)
vertices = ab.distinct()
vertices = vertices.withColumn('id', vertices.src).drop('src')

# COMMAND ----------

from graphframes import *

# COMMAND ----------

g = GraphFrame(vertices, edges)

# COMMAND ----------

print("Number of Airports: ", g.vertices.count())
print("Number of US to US Routes: ", g.edges.count())

# COMMAND ----------

g.find("(a)-[e]->(b); (b)-[e2]->(a)").filter("b.id == 'DEN'").show()

# COMMAND ----------

g.shortestPaths(landmarks=['DEN'])

# COMMAND ----------


