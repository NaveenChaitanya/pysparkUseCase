from pyspark import SparkContext
def main():
    sc = SparkContext(master="local", appName="UseCase8")
    sc.setLogLevel("ERROR")
    data = sc.textFile("file:/home/hduser/SparkUsecase/data.csv").map(lambda x:x.split(",")).map(lambda x:(x[0],x[1]))
    marks = sc.textFile("file:/home/hduser/SparkUsecase/marks.csv").map(lambda x: x.split("~")).map(lambda x:(x[0],(x[1],x[2],x[3])))

    student=data.join(marks)
    studMarks_reduce = student.map(lambda  x: (x[0] ,x[1][0],x[1][1][0],x[1][1][1],x[1][1][2]))


    print("** Student marks after flattening the tuple ***")
    studMarks_reduce.foreach(print)

    studMarks_Total = studMarks_reduce.map(lambda x: (x[0],x[1],int(x[2]),int(x[3]),int(x[4]),int(x[2])+int(x[3])+int(x[4])))
    print("*** Total Marks in all subjects  ****")
    studMarks_Total.foreach(print)

    studMarks_Sort = studMarks_Total.sortBy( lambda x: x[5],False,1)

    print(" Sort the data based on the Total Marks in Descending Order with 1 partition ****")
    studMarks_Sort.foreach(print)

    studMarks_Max = studMarks_Sort.first()
    print(f"Maximum of Total Marks : {studMarks_Max}")

    studMarks_Sort_asc = studMarks_Total.sortBy( lambda  x : x[5],True,1)

    studMarks_Min = studMarks_Sort_asc.first()
    print(f"Minimum of Total Marks : {studMarks_Min}")







main()