# Databricks notebook source
# Adam Brenner: Lab 3

sc = spark.sparkContext

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:///FileStore/tables/lab3short")

#Number of Files you have in your directory.
n = 2

for i in range(n):
  dbutils.fs.cp("dbfs:///FileStore/tables/shortLab3data" + str(i) + ".txt", "dbfs:///FileStore/tables/lab3short")
  


# COMMAND ----------

files = sc.textFile("dbfs:///FileStore/tables/lab3short")

#print(files.collect())
#pairs = lines.map(lambda x: (x.split(" ")[0], x))

pages = files.map(lambda l: (l.split(" ")[0], l.split(" ")[1:]))

# Testing Code

#print(pages.collect())
#print("Keys \n", pages.keys().collect())
#print("Values \n", pages.values().collect())

# y = 'www.example5.com'


# a = []
# b = []
# c = []
# for x in pages.values().collect():
#   if y in x:
#     a.append(x)

# print(a)
# for e in a:
#   if e in pages.values().collect():
#     b.append(e)

# n = pages.count()
# print(n)

# for i in range(n):
#   for x in b:
#     if x in pages.collect()[i]:
#       links = pages.keys().collect()[i]
#       c.append(links)
      
# print(c)

z2 = []
for y in pages.keys().collect():
  z = pages.filter(lambda sub, ele = y: ele in sub[1])
#print(z.collect())
#print(z.keys().collect())
  z2.append((y, z.keys().collect()))
print(z2)

# COMMAND ----------


