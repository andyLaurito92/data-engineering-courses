from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

# This creates an RDD
lines = spark.sparkContext.textFile("file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# The age row comes from the row positional argument coming from the mapper :)
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 and age <= 19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

# Like closing the connection with a database
spark.stop()
