from pyspark import SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase17")
    sc.setLogLevel("ERROR")
    sales=sc.textFile("file:/home/hduser/SparkUsecase/sales.txt")
    header=sales.first()
    data=sales.filter(lambda x: x != header).map(lambda x:x.split(",")).map(lambda x: (x[0],x[1],int(x[2]),x[3]))
    df=data.map(lambda x: ((x[0],x[1],x[3]),(x[2],1)))
    df.foreach(print)

    print("Summary:")
    summary=df.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])).map(lambda x: x[0][0]+","+x[0][1]+","+x[0][2]+","+str(x[1][0])+","+str(x[1][1]))
    head=sc.parallelize(["Dept,Desg,State,empcount,totalcost"])
    head.union(summary).foreach(print)




main()