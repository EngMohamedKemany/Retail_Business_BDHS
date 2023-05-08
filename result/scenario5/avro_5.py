# spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.1 avro.py

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local[*]") \
        .getOrCreate()

import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F

path = 'C:/Users/DevAdmin/MohyWorkSpace/data-files/products_avro/*.avro'
df = spark.read.format("avro").load(path)
df_filter = df.filter(df.product_price > 1000.0)
df_filter.write.mode('Overwrite') \
.option("compression", "snappy") \
.parquet('C:/Users/DevAdmin/MohyWorkSpace/Retail_Proj/result/scenario5/solution')

print('\n')
print('\n')
print("done")
