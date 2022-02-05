from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase5")
    sc.setLogLevel("ERROR")

    user=sc.textFile("file:/home/hduser/SparkUsecase/user.csv")
    header=user.first()

    rdd=user.filter(lambda x : x!=header)\
    .filter(lambda x : "myself" not in x)

    print("Filter header and myself")
    rdd.foreach(print)



main()