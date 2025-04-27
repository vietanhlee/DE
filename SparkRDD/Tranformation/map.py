from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import upper

conf = SparkConf().setAppName("DE-103").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("G:\DE-ETL\SparkRDD\Tranformation\Data\data.txt")

upperRdd = fileRdd.map(lambda line: line.upper())
# print(upperRdd.collect())

for line in upperRdd.collect():
    print(line)

