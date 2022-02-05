from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    auction=sc.textFile("file:/home/hduser/sparkdata/auctiondata").map(lambda  x :x.split("~"))
    print("Total # of auctions:",auction.count())

    #bids / item type
    print("Bids per Item type")
    auction.map(lambda x: (x[7],1)).reduceByKey(lambda x,y : x+y).foreach(print)

    data=auction.map(lambda x : (x[0],1)).reduceByKey(lambda x,y: x+y)
    data.foreach(print)
    #metadata info not clear
    print(data.max())
    print(data.min())





main()