# Databricks notebook source
# Adam Brenner: Lab 2 #

# Primes Exercise
prime_List = [i for i in range(100,10001)]

sc = spark.sparkContext
primeRdd = sc.parallelize(prime_List)

def isPrime(n):
  if n < 2:
    return False
  if n == 2:
    return True
  for i in range(3, n, 2):
    if n % i == 0:
      return False
  return True

#print(isPrime(567))
#print(isPrime(107))
print(primeRdd.filter(isPrime).count())

# COMMAND ----------

# Celsius Exercise
import random

fheit = random.sample(range(0,101),100)

fheitRdd = sc.parallelize(fheight)

#print(fheitRdd.collect())
celsius = fheitRdd.map(lambda x: (x-32)*(5/9))
#can use reduce(x,y: x+y).persist()
#print(celsius.collect())

count = celsius.filter(lambda x: x > 0).count())
aboveFreezing = celsius.filter(lambda x: x > 0).sum()
print("Average of all Celsius values above freezing: ", aboveFreezing / count)

# COMMAND ----------
