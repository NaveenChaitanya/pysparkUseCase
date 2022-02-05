from pyspark import  SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase2")
    sc.setLogLevel("ERROR")
    #File
    file =sc.textFile("hdfs://localhost:54310/user/hduser/SparkCoreUsecase/Inceptez2.txt")
    InceptezLines=file.filter(lambda x : "Inceptez" in x)
    print(f"Inceptez lines count: {InceptezLines.count()}")
    NonInceptezLines =file.subtract(InceptezLines)
    print(f"NonInceptez lines count: {NonInceptezLines.count()}")



main()