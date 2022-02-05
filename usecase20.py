from _csv import QUOTE_ALL

from pyspark import SparkContext
from csv import reader
def lines(x):
    for l in reader(x,  delimiter=','):
        if len(l[0])>1:
            #print (l[0])
            return l[0]





def main():
    sc = SparkContext(master="local", appName="UseCase20")
    sc.setLogLevel("ERROR")
    raw = sc.textFile("file:/home/hduser/SparkUsecase/websales.txt")
    head = raw.first()
    filterdata = raw.filter(lambda x: x != head)
    # filterdata.foreach(print)
    # order_id,account_id,item_id,status,note,reservation_timestamp,item_description,item_name,item_price,item_type

    data = filterdata.map(lambda x: str(x).replace(str(lines(x) ),"") ).map(lambda x : x.split(","))
    price=data.map(lambda x :((x[7],x[9]),float(x[8])))
    print( price.max(lambda x: x[1]))
    price.groupByKey().map(lambda x : (x[0][0],x[0][1],list(x[1]))).foreach(print)

main()
