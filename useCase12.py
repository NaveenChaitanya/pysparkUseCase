from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    content = sc.textFile("file:/home/hduser/SparkUsecase/content.txt").map(lambda x : x.split(" ")).flatMap(lambda x: x)
    remove = sc.textFile("file:/home/hduser/SparkUsecase/remove.txt").collect()

    words=content.filter(lambda x : x not in remove).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).collect()
    print(words)





main()