from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase5")
    sc.setLogLevel("ERROR")
    # File
    fileA = sc.textFile("file:/home/hduser/SparkUsecase/Inceptez4A.txt")
    fileB= sc.textFile("file:/home/hduser/SparkUsecase/Inceptez4B.txt")
    fileC = sc.textFile("file:/home/hduser/SparkUsecase/Inceptez4C.txt")

    file=fileA.union(fileB).union(fileC)
    words=file.map(lambda x: x.split(" ")).flatMap(lambda x: x)
    print(f"Count of words:{words.count()}")






main()