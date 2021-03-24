# Databricks notebook source
# Adam Brenner

sc = spark.sparkContext

# COMMAND ----------

test_list = sc.parallelize(["a b c", "b a a", "c b", "d a"])
test_list_2 = sc.parallelize(["a b", "c d", "b e", "g t y", "x u", "e r"])
links = test_list.map(lambda x: (x.split(" ")[0], x.split(" ")[1:]))

links = links.map(lambda x: (x[0], list(set(x[1])))).persist()

rankings = test_list.map(lambda x: x[0])

n = rankings.count()
rankings = rankings.map(lambda x: (x, 1/n))

print(links.collect())
print(rankings.collect())

# COMMAND ----------

iterations = 10
i = 0
links_rankings = links.join(rankings)
print(links_rankings.collect())
print(links_rankings.values().collect())
while i < (iterations + 1):
  i += 1
  score = links_rankings.values().flatMap(lambda neigh: [(x, neigh[1] / len(neigh[0])) for x in neigh[0]]).reduceByKey(lambda x, y: x+y)
  print(score.collect())

# COMMAND ----------


