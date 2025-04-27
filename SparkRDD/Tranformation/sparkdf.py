from pyspark.shuffle import GroupByKey
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
#create SparkSession

spark = SparkSession.builder \
    .appName("DE-ETL") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
sc = spark.sparkContext
# schemaJson = StructType([
#     StructField("id", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("actor", StructType([
#         StructField("id", LongType(), True),
#         StructField("login", StringType(), True),
#         StructField("gravatar_id", StringType(),True),
#         StructField("url",StringType(),True),
#         StructField("avatar_url", StringType(),True)
#     ]) ,
#     nullable= True)
# ])

# jsonData = spark.read.schema(schemaJson).json("/2015-03-01-17.json")
# # jsonData.show(truncate=False)
# #
# jsonData.select(col("id"), col("type"), col("actor.id").alias("actor_id"),
#                 col("actor.login").alias("actor_login"), 
#                 col("actor.gravatar_id").alias("actor_gravatar_id"), 
#                 col("actor.url").alias("actor_url"), 
#                 col("actor.avatar_url").alias("actor_avatar_url")) \
#                 .show(truncate=True, )

schemaJson = StructType([
    StructField("id", dataType= StringType(), nullable= True),
    StructField("type", dataType= StringType(), nullable= True),
    StructField("actor", dataType= StructType([
        StructField("id", dataType= LongType(), nullable= True),
        StructField("login", dataType= StringType(), nullable= True),
        StructField("gravatar_id", dataType= StringType(), nullable= True),
        StructField("url", dataType= StringType(), nullable= True),
        StructField("avatar_url", dataType= StringType(), nullable= True),
    ]), nullable= True)
])

jsonData = spark.read.schema(schemaJson).json("/2015-03-01-17.json")
js = jsonData.select(col('actor.id').alias('actor_id'),
                    col('actor.login').alias('actor_login'),
                    col('actor.gravatar_id').alias("actor_gravatar_id"),
                    col('actor.url').alias("actor_url"),
                    col('actor.avatar_url').alias("actor_avartar_url")
)

data = [('levietanh', 18),
        ('levana', 19),
        ('levietanh', 20)]
#
# jss = spark.read.schema(schema= StructType([
#     StructField('name')
# ]),
# js.show(truncate=False)

# rdd = sc.parallelize(data)\
#     .reduceByKey(lambda x, y: x + y)
# print(rdd.collect())

schema = StructType([
    StructField('hoten', dataType= StringType(), nullable= True),
    StructField(name= 'tuoi', dataType= LongType(), nullable= True),
])

sparkdf = spark.createDataFrame(data, schema)

sparkdf.select(['hoten']).show()