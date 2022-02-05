from pyspark import  SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase3")
    sc.setLogLevel("ERROR")
    l = ["We", "Are" ,"Learning" , "Hadoop" , "From" , "Inceptez" , "We", "Are" ,"Learning" , "Spark" , "From" , "Inceptez.com" , "hadoop" , "HADOOP"]
    rdd = sc.parallelize(l)
    print(f"Count of Words {rdd.count()}")

    Non_Hadoop = rdd.filter(lambda x :  x.upper()!="HADOOP"  )

    print(f"Non Haddop Words Count: {Non_Hadoop.count()}")




main()