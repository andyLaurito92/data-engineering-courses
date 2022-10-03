### Goal: Try to find the min/max temperature wether the course of a year per station temperature

## With RDDs


## With DataFrames

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
                      StructField("stationID", StringType(), True), \
                      StructField("date", IntegerType(), True), \
                      StructField("measure_type", StringType(), True), \
                      StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv("file:///users/andylaurito/desktop/data-engineering/taming-big-data-apache-spark-python/materials/1800.csv")
df.printSchema()

minTemperaturePerStation = df\
    .filter(df.measure_type == "TMIN")\
    .select("stationID", "temperature")\
    .groupBy("stationID")\
    .min("temperature")\
    .withColumnRenamed("min(Temperature)", "minTemperature")
minTemperaturePerStation.show()

minTemperatureInFarenheit = minTemperaturePerStation\
    .withColumn("temperature", F.round(F.col("minTemperature") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
    .select("stationId", "temperature")\
    .sort("temperature")
minTemperatureInFarenheit.show()

## If you want to bring the data into your driver
#results =  minTemperatureInFarenheit.collect()

spark.stop()
