### Goal: To count the currency of words in book.txt

## Implementation using RDDs
# import re
# from pyspark import SparkConf, SparkContext

# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf=conf)

# lines = sc.textFile("file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/book.txt")

# words = lines.flatMap(lambda x: re.compile(r'\W+', re.UNICODE).split(x.lower()))
# #wordsCounts = words.countByValue()
# wordsCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# wordCountsSorted = wordsCounts.map(lambda x: (x[1], x[0])).sortByKey()

# results = wordCountsSorted.collect()
# for result in results:
#     count = str(result[0])
#     cleanWord = result[1].encode('ascii', 'ignore')
#     #cleanWord = word.encode('ascii', 'ignore')
#     if (cleanWord):
#         print(cleanWord, count)


## Implementation using DataFrames

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
book = spark.read.text("file:///users/andylaurito/desktop/data-engineering/taming-big-data-apache-spark-python/materials/book.txt")
book.show()

words = book\
    .select(F.explode(F.split(book.value, "\\W+")).alias("word"))
words.show()

lowercaseWords = words\
    .filter(words.word != "")\
    .select(F.lower(words.word).alias("word"))
lowercaseWords.show()

wordCounts = lowercaseWords.groupBy("word").count()
wordCounts.show()

wordCountsSorted = wordCounts.sort(F.desc("count"))
wordCountsSorted.show(wordCountsSorted.count())
