from pyspark import SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase16")
    sc.setLogLevel("ERROR")
    employee = sc.textFile("file:/home/hduser/SparkUsecase/EmlpoyeeName.csv").map(lambda x: x.split(",")).map(
        lambda x: (x[0], x[1])).sortBy(lambda x: x[1], True, 1).map(lambda x :x[0]+","+x[1])
    header=sc.parallelize(["Id,Name"])
    header.union(employee).coalesce(1).saveAsTextFile("file:/home/hduser/SparkUsecase/EmlpoyeeSort")




main()