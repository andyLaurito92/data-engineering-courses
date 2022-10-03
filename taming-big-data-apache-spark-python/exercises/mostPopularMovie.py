## GOAL: To calculate the most popular movie. For now, this equals to count the movie most reated :)

FILE = "file:///Users/andylaurito/Desktop/data-engineering/taming-big-data-apache-spark-python/materials/ml-100k/u.data"

## Using DataFrames
# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.types import StructField, StructType, TimestampType, IntegerType, LongType

# try:
#     session = SparkSession\
#         .builder\
#         .appName("MostPopularMovie")\
#         .getOrCreate()

#     schema = StructType([ \
#                           StructField("userid", IntegerType(), False), \
#                           StructField("movieid", IntegerType(), False), \
#                           StructField("rating", IntegerType(), False), \
#                           StructField("timestamp", LongType(), False)\
#                           ])
#     movies = session\
#         .read\
#         .option("delimiter", "\t")\
#         .schema(schema)\
#         .csv(FILE)
#     movies.printSchema()
#     movies.withColumn("timestamp", movies["timestamp"].cast(TimestampType()))
#     movies.show()
    ## Using spark functions

#     moviesByRating = movies\
#         .select("movieid", "rating")\
#         .groupBy("movieid")\
#         .agg(F.sum("rating").alias("sumRating"))
#     moviesByRating.show()

#     moviesOrderedByRating = moviesByRating.sort(F.desc("sumRating"))
#     moviesOrderedByRating.show()

#     mostPopularMovie = moviesOrderedByRating.take(1)
#     print(mostPopularMovie)

    # Using DataFrames with sql
#     movies.createTempView("movies")
#     mostPopularMovie = session.sql("WITH moviesbySumRating AS ("\
#                                    "SELECT movieid, SUM(rating) as sumRating FROM movies GROUP BY movieid)"\
#                                    "SELECT movieid, sumRating FROM moviesBySumRating WHERE sumRating = (SELECT MAX(sumRating) FROM moviesBySumRating)")
#     mostPopularMovie.show()
# finally:
#     session.stop()


## Using mapreduce and RDDs :)

from pyspark.sql import SparkSession, Row

def parseLines(line):
    values = line.split("\t")
    return Row(userid=int(values[0]),\
               movieid=int(values[1]),\
               rating=int(values[2]),\
               timestamp=int(values[3]))
               

session = SparkSession.builder.appName("MostPopularMovieWithRDDs").getOrCreate()
lines = session.sparkContext.textFile(FILE)
movies = lines.map(parseLines)
moviesSumRating = movies\
    .map(lambda row: (row["movieid"], row["rating"]))\
    .reduceByKey(lambda rating1, rating2: rating1 + rating2)\
    .sortBy(lambda row: row[1], False)

for movie in moviesSumRating.collect():
    print(movie)

print("MoviesSumRating type: {}".format(type(moviesSumRating)))
print("Most popular movie: {}".format(moviesSumRating.take(1)))

session.stop()
