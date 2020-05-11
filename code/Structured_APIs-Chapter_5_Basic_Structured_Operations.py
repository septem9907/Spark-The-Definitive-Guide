# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark-book").getOrCreate()

spark

# %%
df = (
    spark
    .read
    .format("json")
    .load("../data/flight-data/json/2015-summary.json")
)

# %%
spark.read.format("json").load("../data/flight-data/json/2015-summary.json").schema

# %%
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])

df = (
    spark
    .read
    .format("json")
    .schema(myManualSchema)
    .load("../data/flight-data/json/2015-summary.json")
)

# %%
from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")

# %%
from pyspark.sql.functions import expr
expr("(((someCol + 5) * 200) - 6) < otherCol")

# %%
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

# %%
myRow[0]
myRow[2]

# %%
df = (
    spark
    .read
    .format("json")
    .load("../data/flight-data/json/2015-summary.json")
)

df.createOrReplaceTempView("dfTable")

# %%
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()

# %%
df.select("DEST_COUNTRY_NAME").show(2)

# %%
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

# %%
from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)

# %%
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

# %%
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)

# %%
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

# %%
df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)

# %%
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

# %%
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)

# %%
df.withColumn("numberOne", lit(1)).show(2)

# %%
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)

# %%
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

# %%
dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))

# %%
dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)

# %%
dfWithLongColName.select(expr("`This Long Column-Name`")).columns

# %%
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)

# %%
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

# %%
df.select("ORIGIN_COUNTRY_NAME").distinct().count()

# %%
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

# %%
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False

# %%
from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)

# %%
df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()

# %%
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# %%
from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# %%
spark.read.format("json").load("/data/flight-data/json/*-summary.json")\
  .sortWithinPartitions("count")

# %%
df.limit(5).show()

# %%
df.orderBy(expr("count desc")).limit(6).show()

# %%
df.rdd.getNumPartitions() # 1

# %%
df.repartition(5)

# %%
df.repartition(col("DEST_COUNTRY_NAME"))

# %%
df.repartition(5, col("DEST_COUNTRY_NAME"))

# %%
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

# %%
collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()
