### Goal: To calculate per customer total spent 

FILE = "file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/customer-orders.csv"

## Implementing solution using RDDs
# from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("TotalAmountSpentByCustomer")
# sc = SparkContext(conf=conf)


# lines = sc.textFile(FILE)

# customerShoppings = lines.map(lambda aLine: aLine.split(",")).map(lambda splittedLine: (int(splittedLine[0]), float(splittedLine[2])))
# totalAmountSpentByCustomer = customerShoppings.reduceByKey(lambda x, y: x + y).sortByKey()
# orederedBySpent = totalAmountSpentByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

# results = orederedBySpent.collect()
# for result in results:
#     print("{:.2f}: {}".format(result[0],  result[1]))


## Implementing solution using DataFrames

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, FloatType, IntegerType

session = SparkSession.builder.appName("AmountPerCustomer").getOrCreate()

customerSchema = StructType([\
                             StructField("id", IntegerType(), True), \
                             StructField("purchaseId", IntegerType(), True), \
                             StructField("amount", FloatType(), True) ])

customers = session.read.schema(customerSchema).csv(FILE)

spentByCustomer = customers\
    .select("id", "amount")\
    .groupBy("id")\
    .sum("amount")\
    .withColumnRenamed("sum(amount)", "totalSpent")

spentByCustomer.show()

sortedBySpentAmount = spentByCustomer\
    .sort(F.desc("totalSpent"))\
    .select("id", F.round(F.col("totalSpent"), 2).alias("totalSpent"))
sortedBySpentAmount.show()

session.stop()
