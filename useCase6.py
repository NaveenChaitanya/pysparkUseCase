from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase6")
    sc.setLogLevel("ERROR")
    txns=sc.textFile("file:/home/hduser/hive/data/txns")
    #"Id", "Date", "CustId", "Amount", "Category", "Product", "City", "State", "PaymentMethod"
    texas=txns.map(lambda x: x.split(",")).filter(lambda x : x[7]=="Texas")
    #Sum of sales
    sales= texas.map(lambda x : (float(x[3])))
    print(f"Sum of Texas Sales:{round(sales.sum(),2)}")

    # Max No of Sales
    print(f"Max of Texas Sales:{round(sales.max(), 2)}")

    #print(f"Sum sales for Texas:{}")
    texas_payType=texas.map(lambda x : (x[8],float(x[3])))
    #Payment type
    print("sales amount group by Payment type:")
    texas_payType.reduceByKey(lambda x,y: x+y).foreach(print)



main()