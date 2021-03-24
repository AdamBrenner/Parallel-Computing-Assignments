# Databricks notebook source
# Adam Brenner
# Assignment 2

sc = spark.sparkContext

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField, StructType, LongType, FloatType
from pyspark.ml.feature import IndexToString, StringIndexer, Bucketizer, VectorIndexer
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.streaming import StreamingContext

# COMMAND ----------

# # Create directory for Lab 8
# dbutils.fs.mkdirs("dbfs:///FileStore/tables/assignment2")

# # Uncomment the following lines if you have not moved the 2 csv files required
# dbutils.fs.mv("dbfs:///FileStore/tables/wildfires.csv", "dbfs:///FileStore/tables/assignment2")

# COMMAND ----------

wildfireSchema = StructType( \
    [StructField('zero', StringType(), True), \
     StructField('one', StringType(), True), \
     StructField('two', StringType(), True), \
     StructField('fire_size', FloatType(), True), \
     StructField('fire_class', StringType(), True), \
     StructField('fire_cause', StringType(), True), \
     StructField('latitude', FloatType(), True), \
     StructField('longitude', FloatType(), True), \
     StructField('state', StringType(), True), \
     StructField('discovery_date', StringType(), True), \
     StructField('contained_date', StringType(), True), \
     StructField('discovery_month', StringType(), True), \
     StructField('ddateFinal', StringType(), True), \
     StructField('cdateFinal', StringType(), True), \
     StructField('putoutTime', StringType(), True), \
     StructField('ddatePre', StringType(), True), \
     StructField('discovery_year', StringType(), True), \
     StructField('dpreMonth', StringType(), True), \
     StructField('wStation', StringType(), True), \
     StructField('dStation', StringType(), True), \
     StructField('wStation2', StringType(), True), \
     StructField('wStation3', StringType(), True), \
     StructField('wStation4', StringType(), True), \
     StructField('vegetation', LongType(), True), \
     StructField('fire_magnitude', FloatType(), True), \
     StructField('weatherFile', StringType(), True), \
     StructField('temp30', StringType(), True), \
     StructField('temp_discovery', FloatType(), True), \
     StructField('temp7', StringType(), True), \
     StructField('tempCont', FloatType(), True), \
     StructField('wind30', StringType(), True), \
     StructField('wind_discovery', FloatType(), True), \
     StructField('wind7', StringType(), True), \
     StructField('windCont', FloatType(), True), \
     StructField('humid30', StringType(), True), \
     StructField('humid_discovery', FloatType(), True), \
     StructField('humid7', StringType(), True), \
     StructField('humidCont', FloatType(), True), \
     StructField('prec30', StringType(), True), \
     StructField('precip_discovery', FloatType(), True), \
     StructField('prec7', StringType(), True), \
     StructField('precipCont', FloatType(), True), \
     StructField('remoteness', FloatType(), True),
])

# COMMAND ----------

wildfireDF = spark.read.format("csv").option("header", True).schema(wildfireSchema).load("dbfs:///FileStore/tables/assignment2/wildfires.csv")

# COMMAND ----------

# Dropping Columns
drop_columns = ['zero', 'one', 'two', 'contained_date', 'ddateFinal', 'cdateFinal', 'putoutTime', 'ddatePre', 'dpreMonth', 'wStation', 'dStation', \
               'wStation2', 'wStation3', 'wStation4', 'weatherFile', 'temp30', 'temp7', 'wind30', 'wind7', \
               'humid30', 'humid7', 'prec30', 'prec7']

wildfireDF = wildfireDF.drop(*drop_columns)

# COMMAND ----------

# Dropping all rows that have null values
wildfireDF = wildfireDF.filter(f.col('fire_cause') != 'Missing/Undefined')
wildfireDF = wildfireDF.dropna()

# COMMAND ----------

# Checking all columns for null values
Dict_Null = {col:wildfireDF.filter(wildfireDF[col].isNull()).count() for col in wildfireDF.columns}
Dict_Null

# COMMAND ----------

wildfireDF.count()

# COMMAND ----------

# Splitting our wildfire dataframe into testing and training sets
train, test = wildfireDF.randomSplit([.8, .2])
print(train.count(), test.count())

# COMMAND ----------

# Below zero wind, humidity, and precipitation didn't make much sense
train = train.withColumn('wind_discovery', f.when(f.col('wind_discovery') < 0, 0).otherwise(f.col('wind_discovery')))
train = train.withColumn('windCont', f.when(f.col('wind_discovery') <= 0, 0).otherwise(f.col('windCont')))
train = train.withColumn('humid_discovery', f.when(f.col('wind_discovery') <= 0, 0).otherwise(f.col('humid_discovery')))
train = train.withColumn('humidCont', f.when(f.col('wind_discovery') <= 0, 0).otherwise(f.col('humidCont')))
train = train.withColumn('precip_discovery', f.when(f.col('wind_discovery') <= 0, 0).otherwise(f.col('precip_discovery')))
train = train.withColumn('precipCont', f.when(f.col('wind_discovery') <= 0, 0).otherwise(f.col('precipCont')))

# COMMAND ----------

# To be used for bucketizers
train.describe('temp_discovery', 'wind_discovery', 'humid_discovery', 'precip_discovery').show()

# COMMAND ----------

# Creating bucketizers for temp, wind, humidity, and precipitation
tempSplits = [-40, 0, 10, 20, 30, 40, 50, 60]
windSplits = [-5, 2, 4, 6, 8, 10, 15, 20, 25, 30]
humidSplits = [-5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
precipSplits = [-5, 1, 4, 8, 10, 20, 30, 40, 50, 100, 1000, 2000, 3000]

tempBucketizer = Bucketizer(splits = tempSplits, inputCol = 'temp_discovery', outputCol = 'tempBucket')
windBucketizer = Bucketizer(splits = windSplits, inputCol = 'wind_discovery', outputCol = 'windBucket')
humidBucketizer = Bucketizer(splits = humidSplits, inputCol = 'humid_discovery', outputCol = 'humidBucket')
precipBucketizer = Bucketizer(splits = precipSplits, inputCol = 'precip_discovery', outputCol = 'precipBucket')

# COMMAND ----------

# Creating features lists to send to our indexers and vector assemblers
# Then creating a random forest classifier and converting our predictions back to labels
features = ['fire_class', 'discovery_month', 'discovery_year']
features2 = ['fire_size', 'latitude', 'longitude', 'vegetation', 'fire_magnitude', 'temp_discovery', 'wind_discovery', 'humid_discovery',
             'precip_discovery', 'remoteness', 'tempBucket', 'windBucket', 'humidBucket', 'precipBucket', 'fire_class_index', 
             'discovery_month_index', 'discovery_year_index']

labelIndexer = StringIndexer(inputCol = 'fire_cause', outputCol = 'label').fit(train)
featureIndexer = [StringIndexer(inputCol = column, outputCol = column + "_index").fit(train) for column in features]

assembler = VectorAssembler(inputCols = features2, outputCol = "features")

rf = RandomForestClassifier(labelCol = 'label', featuresCol = 'features', impurity='gini', maxDepth=10, numTrees=35, featureSubsetStrategy='auto')

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels = labelIndexer.labels)

# Stages for our pipeline below
stag2 = featureIndexer + [labelIndexer, tempBucketizer, windBucketizer, humidBucketizer, precipBucketizer, assembler, rf, labelConverter]

# COMMAND ----------

pl2 = Pipeline(stages=stag2)

# Training pipeline on the training data
plTraining2 = pl2.fit(train)

# Testing pipeline on the test data
predTest2 = plTraining2.transform(test)

# COMMAND ----------

# Selecting our test fire cause labels, our predictions from the random forest
# and finally our predictions converted back to their original labels.
predTest2.select('fire_cause', 'prediction', 'predictedLabel').show(100, False)

# COMMAND ----------



# COMMAND ----------

# This shows us our Random Forest accuracy.
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predTest2)
print("Test Error = %g" % (1.0 - accuracy))

# COMMAND ----------

# Setting up the streaming
trainRepart = train.repartition(25)
testRepart = test.repartition(25)
train.dtypes

# COMMAND ----------

train_test_schema = StructType( \
                              [StructField('fire_size', FloatType(), True), \
                              StructField('fire_class', StringType(), True), \
                              StructField('fire_cause', StringType(), True), \
                              StructField('latitude', FloatType(), True), \
                              StructField('longitude', FloatType(), True), \
                              StructField('state', StringType(), True), \
                              StructField('discovery_date' ,StringType(), True), \
                              StructField('discovery_month', StringType(), True), \
                              StructField('discovery_year', StringType(), True), \
                              StructField('vegetation', LongType(), True), \
                              StructField('fire_magnitude', FloatType(), True), \
                              StructField('temp_discovery', FloatType(), True), \
                              StructField('tempCont', FloatType(), True), \
                              StructField('wind_discovery', FloatType(), True), \
                              StructField('windCont', FloatType(), True), \
                              StructField('humid_discovery', FloatType(), True), \
                              StructField('humidCont', FloatType(), True), \
                              StructField('precip_discovery', FloatType(), True), \
                              StructField('precipCont', FloatType(), True), \
                              StructField('remoteness', FloatType(), True),
                              ])

# COMMAND ----------

dbutils.fs.rm("FileStore/tables/wildfireTrain/", True)
dbutils.fs.rm("FileStore/tables/wildfireTest", True)

trainRepart.write.format("csv").option("header", True).save("FileStore/tables/wildfireTrain/")
testRepart.write.format("csv").option("header", True).save("FileStore/tables/wildfireTest/")

# COMMAND ----------

# Source Stream
sourceStreamTrain = spark.readStream.format("csv").option("header", True). \
               option("maxFilesPerTrigger", 1).schema(train_test_schema).load("dbfs:///FileStore/tables/wildfireTrain")

sourceStreamTest = spark.readStream.format("csv").option("header", True). \
               option("maxFilesPerTrigger", 1).schema(train_test_schema).load("dbfs:///FileStore/tables/wildfireTest")

# COMMAND ----------

q2 = plTraining2.transform(sourceStreamTest).groupBy('label').count()

q3 = q2.writeStream.outputMode("complete").format("console").queryName("q2").start()

for x in range(5):
  spark.sql("SELECT label FROM q2").show()

# COMMAND ----------



# COMMAND ----------


