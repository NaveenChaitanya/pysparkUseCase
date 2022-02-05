from pyspark import SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase19")
    sc.setLogLevel("ERROR")
    raw=sc.parallelize(["dog", "tiger", "lion", "cat", "panther", "eagle"])
    raw.map(lambda x: (len(x),x)).reduceByKey(lambda x,y :x+y).foreach(print)



main()