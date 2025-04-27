from pyspark import SparkContext,SparkConf
import time
from random import random, Random

conf = SparkConf().setAppName("DE-103").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

data = ['A', 'B', 'C', 'D']

rdd = sc.parallelize(data, 2)
print(rdd.glom().collect())

#Elon : 1234, Musk: 73873, Donald: 4636, Trump: 772

# def process_partition(iterator):
#     rand = Random(int(time.time()*1000) + Random().randint(0,100))
#     return [f"{name} : {rand.randint(0,1000)}" for name in iterator]
#
# results = rdd.mapPartitions(process_partition)
#
# print((results.collect()))

results = rdd.mapPartitions(
    lambda iterator: map(
        lambda name: f"{name}: {Random(int(time.time()+1000) + Random().randint(0, 1000)).randint(0, 1000)}",
        iterator
    )
)
print(results.collect())


