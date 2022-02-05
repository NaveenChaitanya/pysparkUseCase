from pyspark import SparkContext
def main():
    sc = SparkContext(master="local",appName="UseCase15")
    sc.setLogLevel("ERROR")
    employee=sc.textFile("file:/home/hduser/SparkUsecase/EmlpoyeeName.csv").map(lambda x : x.split(",")).map(lambda x :(x[0],x[1])).sortBy(lambda x: x[0],True,1)
    employeeManager = sc.textFile("file:/home/hduser/SparkUsecase/EmployeeManager.csv").map(lambda x: x.split(",")).map(lambda x :(x[0],x[1]))
    employeeSalary = sc.textFile("file:/home/hduser/SparkUsecase/EmployeeSalary.csv").map(lambda x: x.split(",")).map(lambda x :(x[0],x[1]))

    empData=employee.join(employeeSalary).join(employeeManager).map(lambda x: x[0]+","+x[1][0][0]+","+x[1][0][1]+","+x[1][1])

    header=sc.parallelize(["Id,Name,Salary,ManagerName"])
    EmpData=header.union(empData)
    EmpData.saveAsTextFile("file:/home/hduser/SparkUsecase/EmployeeData")

main()