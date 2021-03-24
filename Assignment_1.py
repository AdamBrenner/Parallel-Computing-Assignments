# Databricks notebook source
# Adam Brenner
# Assignment 1

sc = spark.sparkContext

# COMMAND ----------

# Uncomment the following lines to create a directory and subsequently move the files.

# dbutils.fs.mkdirs("dbfs:///FileStore/tables/Assignment1")

# dbutils.fs.mv("dbfs:///FileStore/tables/geoPoints0.csv", "dbfs:///FileStore/tables/Assignment1/geoPoints0.csv")
# dbutils.fs.mv("dbfs:///FileStore/tables/geoPoints1.csv", "dbfs:///FileStore/tables/Assignment1/geoPoints1.csv")

# COMMAND ----------

import math

files = sc.textFile("dbfs:///FileStore/tables/Assignment1/")
pointsRdd = files.map(lambda l: [l.split(",")[0], (float(l.split(",")[1]), float(l.split(",")[2]))])
x = pointsRdd.map(lambda x: (x[1][0]))
y = pointsRdd.map(lambda y: (y[1][1]))
c = pointsRdd.count()
dist = []

# COMMAND ----------

# Brute Force Approach
# Long running time, duplicates within

dist = []
th = .75

for i in range(c):
  for j in range(1, c):
    x1 = x.collect()[i]
    x2 = x.collect()[j]
    y1 = y.collect()[i]
    y2 = y.collect()[j]
    d = math.sqrt(((x2 - x1)**2) + ((y2 - y1)**2))
    if d < th and d > 0:
      dist.append((pointsRdd.collect()[i][0], pointsRdd.collect()[j][0]))

# COMMAND ----------

# Brute Force Approach
# Distinct points within the .75 threshold

p = ['Pt01', 'Pt14']
for x, y in dist:
  if (x, y) and (y, x) in dist:
    dist.remove((y, x))
print(thresh)
print(len(dist), dist)

# COMMAND ----------

# Broadcast variables for cell size and threshold

cell_size = sc.broadcast(.75)
threshold = sc.broadcast(.75)

# COMMAND ----------

# Putting points into cells
def gridding(x):
  return [(math.floor(x[1][0] / cell_size.value), math.floor(x[1][1] / cell_size.value)), [x[0], (x[1][0], x[1][1])]]

#(5,5) in relation to (4,4)
def offsetPlusPlus(x):
  return [(x[0][0] + 1, x[0][1] + 1), (x[1])]
#(4,5)
def offsetEvenPlus(x):
  return [(x[0][0], x[0][1] + 1), (x[1])]
#(5,4)
def offsetPlusEven(x):
  return [(x[0][0] + 1, x[0][1]), (x[1])]
#(5,3)
def offsetPlusMinus(x):
  return [(x[0][0] + 1, x[0][1] - 1), (x[1])]
#(4,3)
def offsetEvenMinus(x):
  return [(x[0][0], x[0][1] - 1), (x[1])]

# Used for pushing points
grid = pointsRdd.map(lambda x: gridding(x))
gridPlus = grid.map(lambda x: offsetPlusPlus(x))
gridPlus_2 = grid.map(lambda x: offsetEvenPlus(x))
gridPlus_3 = grid.map(lambda x: offsetPlusEven(x))
gridMinus = grid.map(lambda x: offsetPlusMinus(x))
gridMinus_2 = grid.map(lambda x: offsetEvenMinus(x))

# Only pushed points one way in this case to the right
fullGrid = sc.union([grid, gridPlus, gridPlus_3, gridMinus])

fullGrid = fullGrid.reduceByKey(lambda x, y: x + y)
fullGrid.collect()

# COMMAND ----------

# fullGrid.map(lambda x: x[1]).map(lambda x: [x[i] for i in range(0, len(x), 2)]).collect()

# Seperating the points from the grid cells and then finding the distance between the points.
pairsDist = fullGrid.map(lambda x: x[1]).map(lambda x: ([x[i] for i in range(0, len(x), 2)],[x[i][0] for i in range(1, len(x), 2)], [x[i][1] for i in range(1, len(x), 2)])).map(lambda x: [[[(x[0][i], x[0][j]) for i in range(len(x[0])) for j in range(1, len(x[0]))], [math.sqrt((x[1][j] - x[1][i])**2 + (x[2][j] - x[2][i])**2) for i in range(len(x[1])) for j in range(1,len(x[1]))]]]).flatMap(lambda l: [(x[0], x[1]) for x in l]).map(lambda x: [(x[0][i], x[1][i]) for i in range(len(x[0]))]).flatMap(lambda x: x).flatMap(lambda x: [x])

# Filtering out distances that are less than the threshold distance and greater than zero
pairsDist = pairsDist.filter(lambda x: x[1] <= threshold.value and x[1] > 0).distinct().collect()

# Creating a list of points that are within the threshold distance
pairs = []
for x in pairsDist:
  pairs.append(x[0])
 
# Removing pairs that appear twice as (x,y) and (y,x)
for x, y in pairs:
  if (x, y) and (y, x) in pairs:
    pairs.remove((y, x))

print("Threshold Value: ", threshold.value)
print("Length: ", len(pairs))
print("Elements: ", sorted(pairs))

# COMMAND ----------


