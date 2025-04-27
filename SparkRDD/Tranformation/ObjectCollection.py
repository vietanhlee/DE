from pyspark import SparkContext

#Khởi tạo SparkContext
sc = SparkContext("local[*]", "My-Spark")

#create object collection
data = [
    {"id": 1, "name": "A"},
    {"id": 2, "name": "B"},
    {"id": 3, "name": "C"},
    {"id": 4, "name": "D"}
]

#create RDD from data
rdd = sc.parallelize(data)
# print(rdd.collect())
# print((rdd.count()))
# print(rdd.first())
print(rdd.getNumPartitions())
# print(rdd.glom().collect())