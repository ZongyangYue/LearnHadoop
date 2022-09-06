from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    
    return movieNames

#(userID, movieID, rating)
def parseInput(line):
    fields = line.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    movieNames = loadMovieNames()

    lines = spark.read.text("hdfs://user/maria_dev/ml-100k/u.data").rdd
    
    ratingsRDD = lines.map(parseInput)

    ratings = spark.createDataFrame(ratingsRDD).cache()

    #create an ALS collaborative filtering model
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    #print out ratings from user 0
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print((movieNames[rating['movieID']], rating['rating']))

    print("\nTop 20 recommendations:")
    #Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    #Construct a "test" dateframe for user 0 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    #run model on that test data frame for user id 0
    recommendations = model.transform(popularMovies)

    #get top 20 movies with highest predicted rating for user 0
    topRecommendations = recommendations.sort(recommnedations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print(movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()




