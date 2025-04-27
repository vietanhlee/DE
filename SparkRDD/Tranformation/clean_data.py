import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, IntegerType

# Setup
conf = SparkConf().setAppName("DE-103").setMaster("local[*]").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")
spark = SparkSession(sc)

# Dữ liệu mẫu
a = [
    ['2012 *)() 12 -/*& 06'],
    ['01 *&^ 09 ***((((* 2019'],
    ['*&^%$ 15 08 2021 !@#'],
    ['2020 11 ##$$%% 25 ^^&*'],
    ['03 @@## 05 2017'],
    ['09 *(() 12 2020'],
    ['18 &&^%$# 07 2022'],
    ['2018 ()*& 04 %%$# 02'],
    ['12 06 @!#@#%$^ 2016'],
    ['1999 ((( 01 09'],
    ['21 @@ 03 2005'],
    ['04 07 2010 @@@'],
    ['2015 10 ##@@@ 08'],
    ['(()) 06 02 2014'],
    ['13 !@# 11 2011'],
    ['2013 %%$$ 09 01'],
    ['07 (**) 04 2012'],
    ['05 ## 12 2007'],
    ['19 01 2000 **&^'],
    ['2002 @@@ 08 07'],
]

# Tạo dataframe
df = spark.createDataFrame(data=a, schema=['Date origin'])
# Hàm làm sạch
# def clean_data(s: str) -> list:
#     s = re.sub(r"[^0-9]", " ", s)
#     ls = s.split()
#
#     if len(ls[0]) == 4:
#         ls = ls[::-1]
#
#     return ls
#
#
# # Tạo hàm
# clean_data_udf = udf(clean_data, ArrayType(StringType()))
#
# # Tạo cột mẫu xử lý chung để các cột sau lấy data từ cột này, giúp tối ưu bộ nhớ
# df = df.withColumn('cleaned', clean_data_udf(col('Date origin')))
#
# df = df.withColumn('Day', col('cleaned').getItem(0)) \
#        .withColumn('Month', col('cleaned').getItem(1)) \
#        .withColumn('Year',  col('cleaned').getItem(2)) \
#        # .drop('cleaned')
#
# df.show(truncate=False)


# Cách 2:
def processStringDate(s):
    s = re.sub(r"[^0-9]", " ", s)
    parts = s.split()

    if len(parts) >= 3:
        if len(parts[0]) == 4:
            # Năm đứng trước: YYYY MM DD -> đảo lại
            parts = parts[::-1]
    else:
        parts = ["0", "0", "0"]

    return {
        "day": int(parts[0]),
        "month": int(parts[1]),
        "year": int(parts[2])
    }

# Khai báo schema
schema = StructType([
    StructField("day", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

# Định nghĩa UDF
processDateUDF = udf(processStringDate, schema)

# Áp dụng UDF
df_result = df.withColumn("parse_date", processDateUDF(col("Date origin")))\
    .select(
        col("Date origin"),
        col("parse_date.day").alias("Day"),
        col("parse_date.month").alias("Month"),
        col("parse_date.year").alias("Year")
    )

# Show kết quả
df_result.show(truncate=False)