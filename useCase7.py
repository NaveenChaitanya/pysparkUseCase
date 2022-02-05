from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase7")
    sc.setLogLevel("ERROR")
    txns=sc.textFile("file:/home/hduser/hive/data/txns")
    # "Id", "Date", "CustId", "Amount", "Category", "Product", "City", "State", "PaymentMethod"
    #txns.foreach(print)
    txnsRow=txns.map(lambda x: x.split(","))
    txnStateSales=txnsRow.map(lambda x: (x[7],float(x[3]))).reduceByKey(lambda x,y : x+y).sortByKey(True)
    print("Sales by State")
    txnStateSales.foreach(print)

    print("Transaction count by date")
    txnsByDate=txnsRow.map(lambda x: (x[1],1)).reduceByKey(lambda x,y : x+y)
    txnsByDate.sortByKey(True).foreach(print)

    print("Max Transaction on Date")
    x=txnsByDate.max(key=lambda x: x[1])
    print(x)
















main()