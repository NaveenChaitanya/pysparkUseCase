from pyspark import SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase18")
    sc.setLogLevel("ERROR")
    raw=sc.parallelize([ ("Deepak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)])
    summary=raw.map(lambda x :((x[0],x[1]),int(x[2]))).reduceByKey(lambda x,y: x+y).map(lambda  x: x[0][0]+","+x[0][1]+","+str(x[1]))
    head = sc.parallelize(["Name,Gender,Cost"])
    head.union(summary).foreach(print)



main()