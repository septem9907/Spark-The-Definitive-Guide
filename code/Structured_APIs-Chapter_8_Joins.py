# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark-book").getOrCreate()

# %%
spark

# %%
person = (
    spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])
    .toDF("id", "name", "graduate_program", "spark_status")
)

graduateProgram = (
    spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])
    .toDF("id", "degree", "department", "school")
)

sparkStatus = (
    spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])
    .toDF("id", "status")
)

# %%
joinExpression = person["graduate_program"] == graduateProgram['id']

# %%
wrongJoinExpression = person["name"] == graduateProgram["school"]

# %%
joinType = "inner"

# %%
gradProgram2 = graduateProgram.union(spark.createDataFrame([(0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")

# %%
from pyspark.sql.functions import expr

(person.withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)"))
    .show()
)


# %%
