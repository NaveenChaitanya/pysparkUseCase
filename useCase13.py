from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    content = sc.textFile("file:/home/hduser/SparkUsecase/file1.txt,file:/home/hduser/SparkUsecase/file2.txt,file:/home/hduser/SparkUsecase/file3.txt").map(lambda x : x.split(" "))\
        .flatMap(lambda  x: x)
    words=["a","the","an","as","a","with","this","these","is","are","in","for","to","and","The","of"]

    content.filter(lambda x :x not in words ).map(lambda x: (x,1)).reduceByKey(lambda x,y :x+y).sortBy(lambda x: x[1],False).foreach(print)


main()