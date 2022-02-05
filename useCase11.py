from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    movies = sc.textFile("file:/home/hduser/SparkUsecase/movies.txt")
    #id,movieName,year,rating,duration
    head=movies.first()
    movielist=movies.filter(lambda x: x!=head).map(lambda x: x.split(","))

    movielist.foreach(print)

    print("Movies with greater than 3.5 rating")
    movielist.filter(lambda x: float(x[3])>3.5).foreach(print)
    print("Movies released after 1980")
    movielist.filter(lambda x: int(x[2]) > 1980).foreach(print)
    print("Movies list by year")
    moviesyear=movielist.map(lambda x: (x[2],x[1])).groupByKey().map(lambda x: (x[0],list(x[1]))).collect()
    for x in moviesyear:
        print (x)
    #movielist.map(lambda x: (x[2], x[1])).groupByKey().map(lambda x: (x[0], list(x[1]))).saveAsTextFile("hdfs://localhost:54310/user/hduser/SparkCoreUsecase/movies")
    movielist.map(lambda x: (x[2], x[1])).groupByKey().map(lambda x: (x[0], list(x[1]))).saveAsTextFile("file:/home/hduser/SparkUsecase/movies")






main()