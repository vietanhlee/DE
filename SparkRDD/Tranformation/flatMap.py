from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import upper

conf = SparkConf().setAppName("DE-103").setMaster("local[*]").set("spark.executor.memory", "2g")

sc = SparkContext(conf=conf)

fileRdd = sc.textFile("G:\DE-ETL\SparkRDD\Tranformation\Data\data.txt")

wordRdd = fileRdd.flatMap(lambda word: word.split(" "))
# print((wordRdd.collect()))

capitalizedWordRdd = wordRdd.map(lambda word: word.capitalize())
print((capitalizedWordRdd.collect()))
