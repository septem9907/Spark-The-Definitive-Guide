# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark-book").getOrCreate()

spark

# %%
df = spark.range(500).toDF("number")
df.select(df["number"] + 10)

# %%
spark.range(2).collect()

# %%
from pyspark.sql.types import *
b = ByteType()

# %%
