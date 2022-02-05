from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    course=sc.textFile("file:/home/hduser/SparkUsecase/coursefee.txt")
    header=course.first()
    courseData=course.filter(lambda x: x !=header).map(lambda x: x.split(","))
    finalData=courseData.map(lambda x: (x[0],x[1],x[2],float(x[1])*float(x[2])/100,float(x[1])+float(x[1])*float(x[2])/100))
    finalData.saveAsTextFile("hdfs://localhost:54310/user/hduser/SparkCoreUsecase/courseDetails")


main()