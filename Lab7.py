# Databricks notebook source
# Adam Brenner
# Lab 7

sc = spark.sparkContext

# COMMAND ----------

# Create directory for Lab 7
# dbutils.fs.mkdirs("dbfs:///FileStore/tables/lab7Master")

# Uncomment the following lines if you have not moved the 2 csv files required
# dbutils.fs.mv("dbfs:///FileStore/tables/heartTraining.csv", "dbfs:///FileStore/tables/lab7Master")
# dbutils.fs.mv("dbfs:///FileStore/tables/heartTesting.csv", "dbfs:///FileStore/tables/lab7Master")

trainingData = spark.read.option("header", True).csv("dbfs:///FileStore/tables/lab7Master/heartTraining.csv")
testData = spark.read.option("header", True).csv("dbfs:///FileStore/tables/lab7Master/heartTesting.csv")

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, Bucketizer
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, when

# Converting Age and Chol to ints in both the training and testing datasets
trainingData = trainingData.withColumn("age", trainingData['age'].cast('int'))
trainingData = trainingData.withColumn("chol", trainingData['chol'].cast('int'))
testData = testData.withColumn("age", testData['age'].cast('int'))
testData = testData.withColumn("chol", testData['chol'].cast('int'))

# Converting Sex and Pred to numerical using string indexer
sexIndexer = StringIndexer(inputCol="sex", outputCol="sexIndex")
predIndexer = StringIndexer(inputCol="pred ", outputCol="label")

# Converting Sex and Pred to numerical in both training and testing datasets
# trainingData = sexIndexer.fit(trainingData).transform(trainingData)
# trainingData = predIndexer.fit(trainingData).transform(trainingData)

# testData = sexIndexer.fit(testData).transform(testData)
# testData = predIndexer.fit(testData).transform(testData)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, Bucketizer

# Creating an age buckets for, below 40, 40-49, 50-59, 60-69, 70 and above
ageSplits = [0, 40, 50, 60, 70, 150]
ageBucketizer = Bucketizer(splits = ageSplits, inputCol = "age", outputCol = "ageBucket")

# trainingData = ageBucketizer.transform(trainingData)
# testData = ageBucketizer.transform(testData)


# COMMAND ----------

from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(maxIter=10, regParam=.01)

# COMMAND ----------

trainingData.show()
testData.show()

# COMMAND ----------

from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler

# Creating a features vector containing the agebucket, sex index, and chol
vecAssem = VectorAssembler(inputCols=['ageBucket', 'sexIndex', 'chol'], outputCol='features')
# Creating pipeline stages for the pipeline
heartStages = [ageBucketizer, sexIndexer, predIndexer, vecAssem, lr]

# Pipeline creation
pl = Pipeline(stages=heartStages)

# Training pipeline on the training data
plTraining = pl.fit(trainingData)

# Testing pipeline on the test data
predTest = plTraining.transform(testData)
predTest.select('id', 'age', 'sex', 'chol', 'probability', 'prediction').show()

# COMMAND ----------

# Testing the pipeline on the training data
predTrain = plTraining.transform(trainingData)
predTrain.select('id', 'age', 'sex', 'chol', 'probability', 'pred ', 'prediction').show()

# COMMAND ----------


