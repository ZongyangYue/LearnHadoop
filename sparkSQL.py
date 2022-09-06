# Spark 2.0, DataFrame, a DataSet of Row Objects
# use DataSets instead of DataFrames when you can
# DataSets can contain Row or other types of objects
# start a JDBC/ODBC server with sbin/start-thriftserver.sh, port 10000 by default
# connect using bin/beeline - u jdbc:hive2://localhost:10000
# hiveCtx.cacheTable("tableName")

#User-defined functions
#from pysparck.sql.types import IntegerType
#hiveCtx,sql("SELECT square('someNumericField') FROM tableName")

#still worst Movies problem

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    movieNames = loadMovieNames()

    lines = spark.SparkContext.textFile("hdfs://user/maria_dev/ml-100k/u.data")

    movies = lines.map(parseInput)

    movieDataset = spark.createDataFrame(movies)

    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    #(movieID, count)
    counts = movieDataset.groupBy("movieID").count()

    #(movieID, avgRating, count)
    averageAndCounts = counts.join(averageRatings, "movieID")

    #filter out movies rated 10 or fewer times
    popularAveragesAndCounts = averagesAndCounts.filter("count > 10")
    
    topTen = averageAndCounts.orderBy("avg(rating)").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])