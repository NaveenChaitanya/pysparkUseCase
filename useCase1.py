from pyspark import  SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase1")
    sc.setLogLevel("ERROR")
    #File
    file =sc.textFile("hdfs://localhost:54310/user/hduser/SparkCoreUsecase/Inceptez1.txt")
    print("No of lines in Inceptez1.txt",file.count())
main()


