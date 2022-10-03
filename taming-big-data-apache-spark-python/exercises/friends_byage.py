### Calculate the average friend that each age has


from pyspark import SparkConf, SparkContext

## Implementation with RDDs
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/fakefriends.csv")
rdd = lines.map(parseLine)

# Iterate over rows in rdd
# for row in rdd.collect():
#     print(row)

#print("My RDD is {}".format(rdd))

totalsByAge = rdd\
    .mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))\
    .mapValues(lambda x: x[0]/x[1])\
    .sortByKey()
#print("After reduce by key: {}".format(totalsByAge))

results = totalsByAge.collect()
for result in results:
    print("{} : {:.2f}".format(result[0], result[1]))

## Implementation with DataFrames

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("FriendsByAgeWithDataFrames").getOrCreate()
people = spark.read\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .csv("file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/fakefriends-header.csv")

#people.show()

## Using spark functions
people\
    .select(people.age, people.friends)\
    .groupBy(people.age)\
    .agg(F.round(F.avg("friends"), 2).alias("friedsAvg"))\
    .orderBy("age", ascending=True)\
    .show(100, False)

# Another way is using agg as this
#.agg({"friends":"avg"})\


## Using SQL
# people.createTempView("people")
# spark.sql("SELECT age, AVG(friends) as avgFriends FROM people GROUP BY age ORDER BY age ASC").show(100, False)


spark.stop()
