from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    cricket=sc.textFile("file:/home/hduser/SparkUsecase/cricket.txt").map(lambda x: x.split(","))
    football = sc.textFile("file:/home/hduser/SparkUsecase/football.txt").map(lambda x: x.split(","))
    hockey = sc.textFile("file:/home/hduser/SparkUsecase/hockey.txt").map(lambda x: x.split(","))

    #*******cricket and Footbal*******
    #solution 1:
    print("Players in Cricket and Footbal")
    cricket.union(football).map(lambda x: (x[0],1)).reduceByKey(lambda x,y:x+y).filter(lambda x : x[1]==2).foreach(print)

    #solution 2:
    print("*************solution 2 ****************")
    cricket.join(football).map(lambda x: x[0]).foreach(print)

    #********All Sports**********
    print("Players in Cricket, Hockey and Footbal")
    cricket.join(football).join(hockey).map(lambda x : x[0]).foreach(print)
    # ********Distinct players form all Sports**********
    print("Distinct players form all sprots")
    cricket.union(football).union(hockey).map(lambda x: x[0]).distinct(1).foreach(print)







main()

