# Databricks notebook source
# Adam Brenner
# Lab 6

sc = spark.sparkContext

# COMMAND ----------

# Create directory for lab 6 to store the files
dbutils.fs.mkdirs("dbfs:///FileStore/tables/lab6Master")

# Uncomment the following lines if you haven't moved the 3 csv files to the lab6 folder
# dbutils.fs.mv("dbfs:///FileStore/tables/Master_1_.csv", "dbfs:///FileStore/tables/lab6Master")
# dbutils.fs.mv("dbfs:///FileStore/tables/AllstarFull_1_.csv", "dbfs:///FileStore/tables/lab6Master")
# dbutils.fs.mv("dbfs:///FileStore/tables/Teams_1_.csv", "dbfs:///FileStore/tables/lab6Master")

# COMMAND ----------

# Reads in the csv files into dataframes 
master = spark.read.option("header", True).csv("dbfs:///FileStore/tables/lab6Master/Master_1_.csv")
teams = spark.read.option("header", True).csv("dbfs:///FileStore/tables/lab6Master/Teams_1_.csv")
all_star = spark.read.option("header", True).csv("dbfs:///FileStore/tables/lab6Master/AllstarFull_1_.csv")

# Selecting only the columns we want from the 3 dataframes
master = master.select('playerID', 'nameFirst', 'nameLast')
teams = teams.select('teamID', 'name')
all_star = all_star.select('playerID', 'teamID')

print('Master: \n')
master.printSchema()
print('Teams: \n')
teams.printSchema()
print('All Star: \n')
all_star.printSchema()

# COMMAND ----------

# This joins the above 3 dataframes into one with playerID, nameFirst, nameLast, teamID, and team name
masterDF = master.join(all_star, master["playerID"] == all_star["playerID"]).select(master["*"], all_star["teamID"]).distinct()
masterDF = masterDF.join(teams, masterDF["teamID"] == teams["teamID"]).select(masterDF["*"], teams["name"]).distinct()

# COMMAND ----------

# Uncomment if this parquet file doesn't exist yet in the DBFS
# It also partitions the parquet file by 'name'
#masterDF.repartition("name").write.partitionBy("name").parquet("masterP.parquet")
parqMaster = spark.read.parquet("masterP.parquet")

# COMMAND ----------

c = parqMaster.count()
print("Number of Colorado Rockies All Stars: \n",
      parqMaster.filter(parqMaster["name"] == "Colorado Rockies").count())
parqMaster.filter(parqMaster["name"] == "Colorado Rockies").select("nameFirst", "nameLast").show(c, False)

# COMMAND ----------


