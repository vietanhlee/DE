from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-103").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

numbers = [1,2,3,4,5,6,7,8]

numbersRdd = sc.parallelize(numbers)

#thực thi trên tất cả bản ghi
squaredRdd = numbersRdd.map(lambda  x: x * x)
# print(squaredRdd.collect())

#lay cac phan tu lon hon 2
filterRdd = numbersRdd.filter(lambda x: x > 2)
# print(filterRdd.collect())

#bien doi list ve dang [[1,2], [2,4],...]
flatMapRdd = numbersRdd.flatMap(lambda x : [x, x * 2])

print((flatMapRdd.collect()))






