from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparSqlWithSchema").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/fakefriends-header.csv")

# Compare this output with the one with sparsqq-frieds.py. You will see that this dataframe is just a list of rows with columns, same as we build in the other script!
for person in people.collect():
    print(person)

print("Inferred schema")
people.printSchema()

print("Displaying name column")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older")
people.select(people.name, people.age + 10).show()

spark.stop()
