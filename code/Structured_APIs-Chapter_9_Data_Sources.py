# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark-book").getOrCrate()

# %%
spark

# %%
csvFile = spark.read.format("csv")\
    .option("header", "true")\
    .option("mode", "FAILFAST")\
    .option("inferSchema", "true")\
    .load("../data/flight-data/csv/2010-summary.csv")

# %%
csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
    .save("../tmp/my-tsv-file.tsv")

# %%
spark.read.format("json").option("mode", "FAILFAST")\
    .option("inferSchema", "true")\
    .load("../data/flight-data/json/2010-summary.json").show(5)

# %%
csvFile.write.format("json").mode("overwrite").save("../tmp/my-json-file.json")

# %%
spark.read.format("parquet")\
    .load("../data/flight-data/parquet/2010-summary.parquet").show(5)

# %%
csvFile.write.format("parquet").mode("overwrite")\
    .save("../tmp/my-parquet-file.parquet")

# %%
spark.read.format("orc").load("../data/flight-data/orc/2010-summary.orc").show(5)

# %%
csvFile.write.format("orc").mode("overwrite").save("../tmp/my-json-file.orc")

# %%
driver = "org.sqlite.JDBC"
path = "../data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

# %%
dbDataFrame = spark.read.format("jdbc").option("url", url)\
    .option("dbtable", tablename).option("driver", driver).load()

# %%
pgDF = spark.read.format("jdbc")\
    .option("driver", "org.postgresql.Driver")\
    .option("url", "jdbc:postgresql://database_server")\
    .option("dbtable", "schema.tablename")\
    .option("user", "username").option("password", "my-secret-password").load()

# %%
dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain()

# %%
pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
    AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
    .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)\
    .load()

# %%
dbDataFrame = spark.read.format("jdbc")\
    .option("url", url).option("dbtable", tablename).option("driver",  driver)\
    .option("numPartitions", 10).load()

# %%
props = {"driver": "org.sqlite.JDBC"}
predicates = [
    "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
    "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
    .rdd.getNumPartitions() # 2

# %%
props = {"driver": "org.sqlite.JDBC"}
predicates = [
    "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
    "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()

# %%
colName = "count"
lowerBound = 0L
upperBound = 348113L # this is the max count in our database
numPartitions = 10

# %%
spark.read.jdbc(url, tablename, column=colName, properties=props,
                lowerBound=lowerBound, upperBound=upperBound,
                numPartitions=numPartitions).count() # 255

# %%
newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)

# %%
spark.read.jdbc(newPath, tablename, properties=props).count() # 255

# %%
csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)

# %%
spark.read.jdbc(newPath, tablename, properties=props).count() # 765

# %%
csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")\
    .write.partitionBy("count").text("../tmp/five-csv-files2py.csv")

# %%
csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")\
    .save("../tmp/partitioned-files.parquet")


# COMMAND ----------

