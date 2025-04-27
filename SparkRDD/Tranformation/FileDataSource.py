from pyspark import SparkContext, SparkConf

#config spark
conf  = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("G:\DE-ETL\SparkRDD\Tranformation\Data\data.txt")
# print(fileRdd.collect())
# print(fileRdd.getNumPartitions())
# print(fileRdd.count())


