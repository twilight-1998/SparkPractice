from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#senario 1
data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]
df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
df.show()

teamdf=df.select('TeamA').union(df.select('TeamB')).distinct().withColumnRenamed("TeamA","TeamName")
teamdf.show()

wondf=df.groupby("Won").count()
wondf.show()

finaldf=(teamdf.join(wondf,teamdf["TeamName"] == wondf["Won"],"left").fillna(0)
         .select("TeamName","count").withColumnRenamed("count","Won")).orderBy("TeamName")

finaldf.show()
#------------------------------------------------------------------------------------------------------------
#Senario 2 --> pivoting
from pyspark.sql.functions import *
data = [
    (101, "Eng", 90),
    (101, "Sci", 80),
    (101, "Mat", 95),
    (102, "Eng", 75),
    (102, "Sci", 85),
    (102, "Mat", 90)
]
columns = ["Id", "Subject", "Marks"]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
df.show()

marksdf=df.groupby("Id").pivot("Subject").agg((first("Marks")))
marksdf.show()
#------------------------------------------------------------------------------------------------------------
#scenario 3 --> masking
data = [
    ("Renuka1992@gmail.com", "9856765434"),
    ("anrasu@gmail.com", "9844567788")
]
df = spark.createDataFrame(data, ["email", "mobile"])
df.show()

from pyspark.sql.functions import expr
df1 = (
    df
    .withColumn("split_0", expr("split(email, '@')[0]"))
    .withColumn("split_1", expr("split(email, '@')[1]"))
    .withColumn("length", expr("length(split_0)"))
    .withColumn("redlength", expr("length - 3"))
    .withColumn("First", expr("substring(split_0, 1, 1)"))
    .withColumn("last", expr("substring(split_0, length(split_0)-1, 2)"))
    .withColumn(
        "Masking",
        expr("concat(First, repeat('*', redlength), last)")
    )
    .withColumn(
        "Final",
        expr("concat(Masking, '@', split_1)")
    )
)
df1.show(truncate=False)
#------------------------------------------------------------------------------------------------------------
# Git Scenario 1: Query to get who are getting equal salary
from pyspark.sql.functions import *

data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
        ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
        ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
        ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
        ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]

details=spark.createDataFrame(data,myschema)
details.show()

equal_sal=details.groupby("salary").agg(count('salary')).filter("count('salary') > 1")
equal_sal.show()

sal2=details.join(equal_sal, on='salary', how='Inner').select("workerid","firstname","lastname","salary","joiningdate","depart")
sal2.show()

#------------------------------------------------------------------------------------------------------------

# Git Scenario 2:(Need the dates when the status gets changed like ordered to dispatched)
from pyspark.sql.functions import *
data = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (2, "3-Jan", "shipped")]
myschema = ["orderid","statusdate","status"]
df = spark.createDataFrame(data,schema=myschema)
df.show()

ord_disp=df.filter(df.status == "dispatched")
#ord_disp=df.filter("status = 'dispatched'")
ord_disp.show()

#------------------------------------------------------------------------------------------------------------

# Git Scenario 4:(Write a query to list the unique customer names in the custtab table, along with the number of addresses associated with each customer.)
from pyspark.sql.functions import *

data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
myschema = ["custid", "custname", "address"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

custtab=df.groupby("custid","custname").agg(collect_set("address").alias('address')).orderBy("custid")
custtab.show()

#------------------------------------------------------------------------------------------------------------

# Git Scenario 5:
# 1.Check number of partitions,
# 2. Create a new column 'salary' with a constant value 1000 using lit method.
# 3. Join df2 and d3 to form df4.
# 4. Remove invalid email id
# 5. Write the final dataframe to parquet format partitioned by 'salary'
from pyspark.sql.functions import *

data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]
myschema1 = ["id", "name", "age", "email"]
df1 = spark.createDataFrame(data1, schema=myschema1)
df1.show()

data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
myschema2 = ["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2, schema=myschema2)
df2.show()

#number of partitions
partcount=df1.rdd.getNumPartitions()
print("number of partitions in df1:",partcount)

#Creating a new dataframe df3 by adding a new column 'salary' with a constant
# value 1000 using lit method which helps you to add a constant value to a column.

df3=df1.withColumn('salary',lit(1000))
df3.show()

#appand df2 and df3 to form df4
df4=df2.union(df3)
df4.show()

# Remove records which are invalid email from df4
abc=df4.filter("email like '%@%'").orderBy("id")
abc.show()

#write abc to target location
abc.write.format("parquet").partitionBy("salary").save("/Users/twilight/Desktop/Data Engineering/Revision/details")

#------------------------------------------------------------------------------------------------------------

# Git Scenario 6:For Employee salary greater than 10000 give designation as manager else employee
data = [
    ("1", "a", "10000"),
    ("2", "b", "5000"),
    ("3", "c", "15000"),
    ("4", "d", "25000"),
    ("5", "e", "50000"),
    ("6", "f", "7000")
]
myschema = ["empid","name","salary"]
df = spark.createDataFrame(data,schema=myschema)
df.show()

deg=df.withColumn("Designation",expr("case when salary > 10000 then 'Manager' else 'Employee' end "))
deg.show()
#------------------------------------------------------------------------------------------------------------

# Git Scenario 7:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 8:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 9:write spark code, list of name of participants who has rank=1 most number of times
from pyspark.sql.functions import *

data = [
    ("a", [1, 1, 1, 3]),
    ("b", [1, 2, 3, 4]),
    ("c", [1, 1, 1, 1, 4]),
    ("d", [3])
]
df = spark.createDataFrame(data, ["name", "rank"])
df.show()

explodedf = df.withColumn("rank", explode(col("rank")))
explodedf.show()

fildf=explodedf.filter("rank == 1")
fildf.show()

grpdf=fildf.groupBy("name").agg(count("rank").alias('count'))
print(grpdf)

finaldf=grpdf.select("name").first()[0]
print(finaldf)
#------------------------------------------------------------------------------------------------------------
# Git Scenario 10:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 11 :I have a table called Emp_table, it has 3 columns, Emp name, emp ID , salary
# in this I want to get salaries that are >10000 as Grade A, 5000-10000 as grade B and < 5000 as Grade C,
# write an SQL query
from pyspark.sql.functions import *
data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]
df = spark.createDataFrame(data, ["emp_id", "emp_name", "salary"])
df.show()

grade_df=df.withColumn("Grade",expr("""case when salary > 10000 then 'A'
                                    when salary between 5000 and 10000 then 'B'
                                    when salary < 5000 then 'C' end """))
grade_df.show()
#------------------------------------------------------------------------------------------------------------

# Git Scenario 12:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 13 :We have employee id,employee name, department. Need count of every department employees.
from pyspark.sql.functions import *

data = [(1, "Jhon", "Development"),
        (2, "Tim", "Development"),
        (3, "David", "Testing"),
        (4, "Sam", "Testing"),
        (5, "Green", "Testing"),
        (6, "Miller", "Production"),
        (7, "Brevis", "Production"),
        (8, "Warner", "Production"),
        (9, "Salt", "Production")]
df = spark.createDataFrame(data, ["emp_id", "emp_name", "dept"])
df.show()

dep_total=df.groupBy("dept").agg(count("emp_id").alias("Total")).orderBy('Total')
dep_total.show()
#------------------------------------------------------------------------------------------------------------
# Git Scenario 14 :We need total marks
from pyspark.sql.functions import *

data = [
    (203040, "rajesh", 10, 20, 30, 40, 50)
]

df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
df.show()

total_marks_df=df.withColumn("Total",expr("telugu + english + maths + science + social"))
total_marks_df.show()
#------------------------------------------------------------------------------------------------------------

# Git Scenario 15 :Extend and Append list in python and scala
from pyspark.sql.functions import *

l1 = [2, 3, 4, 5]
l2 = [6, 7, 8, 9]

#To Extend
l1.extend(l2)
print(l1)

#To Append
l1.append(l2)
print(l1)
#------------------------------------------------------------------------------------------------------------

# Git Scenario 16 :Remove duplicates
from pyspark.sql.functions import *

data = [(1, "Jhon", "Testing", 5000),
        (2, "Tim", "Development", 6000),
        (3, "Jhon", "Development", 5000),
        (4, "Sky", "Prodcution", 8000)]
df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])
df.show()

drop_dup=df.drop_duplicates(["name"]).orderBy("id")
drop_dup.show()
#------------------------------------------------------------------------------------------------------------

# Git Scenario 17 :(df1 contains Employeeid,Name,Age,State,Country columns df2 contains Employeeid,Name,Age,Address
# columns. how do you merge df1 and df2 to get the following output Employeeid,Name,Age,State,Country,Address)
from pyspark.sql.functions import *

data = [(1, "Tim", 24, "Kerala", "India"),
        (2, "Asman", 26, "Kerala", "India")]
df1 = spark.createDataFrame(data, ["emp_id", "name", "age", "state", "country"])
df1.show()

data2 = [(1, "Tim", 24, "Comcity"),
         (2, "Asman", 26, "bimcity")]
df2 = spark.createDataFrame(data2, ["emp_id", "name", "age", "address"])
df2.show()

df3=df1.join(df2,["emp_id", "name", "age"],"outer")
df3.show()
#------------------------------------------------------------------------------------------------------------

# Git Scenario 19 :Flatten the below complex dataframe

from pyspark.sql.functions import *

data={
    "code": 1234,
    "commentCount": 5,
    "createdAt": "2023-05-30T10:30:00",
    "description": "Example description",
    "feedsComment": "Example comment",
    "id": 1,
    "imagePaths": "/path/to/images",
    "images": "image1.jpg,image2.jpg,image3.jpg",
    "isdeleted": 'false',
    "lat": 123456789,
    "likeDislike": {
        "dislikes": 10,
        "likes": 20,
        "userAction": 1
    },
    "lng": 987654321,
    "location": "Example location",
    "mediatype": 1,
    "msg": "Example message",
    "multiMedia": [
        {
            "createAt": "2023-05-30T12:00:00",
            "description": "Media description",
            "id": 1001,
            "likeCount": 50,
            "mediatype": 1,
            "name": "Media name",
            "place": "Media place",
            "url": "https://example.com/media1"
        },
        {
            "createAt": "2023-05-30T13:30:00",
            "description": "Another media description",
            "id": 1002,
            "likeCount": 30,
            "mediatype": 2,
            "name": "Another media name",
            "place": "Another media place",
            "url": "https://example.com/media2"
        }
    ],
    "name": "John Doe",
    "profilePicture": "/path/to/profile_picture.jpg",
    "title": "Example title",
    "userId": 123,
    "videoUrl": "https://example.com/video",
    "totalFeed": 100
}

df=spark.read.option("multiline","True").json(sc.parallelize([data]))
df.show()
df.printSchema()

flat_df=(df.withColumn("dislikes",expr("likeDislike.dislikes"))
         .withColumn("likes",expr("likeDislike.likes"))
         .withColumn("userAction",expr("likeDislike.userAction"))
         .withColumn("multiMedia",expr("explode(multiMedia)"))
         .withColumn("createAt",expr("multiMedia.createAt"))
         .withColumn("description",expr("multiMedia.description"))
         .withColumn("id",expr("multiMedia.id"))
         .withColumn("likeCount",expr("multiMedia.likeCount"))
         .withColumn("mediatype",expr("multiMedia.mediatype"))
         .withColumn("name",expr("multiMedia.name"))
         .withColumn("place",expr("multiMedia.place"))
         .withColumn("url",expr("multiMedia.url"))
         ).drop("likeDislike","multiMedia")
flat_df.show()
flat_df.printSchema()
#------------------------------------------------------------------------------------------------------------
#Git Scenario 20: Reverse of above
#------------------------------------------------------------------------------------------------------------
#Git Scenario 21:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 22:Cumilative sum
from pyspark.sql.functions import *

data = [(1, "26-May", 100),
        (1, "27-May", 200),
        (1, "28-May", 300),
        (2, "29-May", 400),
        (3, "30-May", 500),
        (3, "31-May", 600)]
df = spark.createDataFrame(data, ["pid", "date", "price"])
df.show()

cumdf=df.withColumn("new_price",expr("sum(price) over (partition by pid order by date)"))
cumdf.show()
#------------------------------------------------------------------------------------------------------------

#Git Scenario 23:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 24: Get list of Pages for each user
from pyspark.sql.functions import *

data = [
    (1, "home"),
    (1, "products"),
    (1, "checkout"),
    (1, "confirmation"),
    (2, "home"),
    (2, "products"),
    (2, "cart"),
    (2, "checkout"),
    (2, "confirmation"),
    (2, "home"),
    (2, "products")]
df = spark.createDataFrame(data, ["userid", "page"])
df.show()

finaldf=df.groupBy("userid").agg(collect_list("page").alias("pages"))
finaldf.show()
#------------------------------------------------------------------------------------------------------------

#Git Scenario 25:consider a file with some bad/corrupt data as shown below.
#How will you handle those and load into spark dataframe

sample_csv_data="emp_no,emp_name,dep"
"101,Murugan,HealthCare"
"Invalid Entry,Description: Bad Record Entry"
"102,Kannan,Finance"
"103,Mani,IT"
"Connection lost,Description: Poor Connection"
"104,Pavan,HR"
"Bad Record,Description:Corrupt Record"

df = spark.read.format("csv").option("header", "true") \
    .option("mode", "DROPMALFORMED") \
    .load("D:/BigData/Datasets/Scenerio25.csv")
df.show()
#Note:
#There are three modes available when reading a file in Spark:
#1. DROPMALFORMED: Drops the rows with bad/Corrupt data
#2. PERMISSIVE: Keeps the rows with bad/Corrupt data.it sets the problematic fields to null and adds a new column
               #called _corrupt_record to store the entire problematic row as a string.(Default if no mode is specified)
#3. FAILFAST: Throws an error if any bad/Corrupt data is found

ifnull(0,2)
#------------------------------------------------------------------------------------------------------------

#Git Scenario 27: Find the difference between the salary of an employee.
from pyspark.sql.functions import *

data = [(1,60000,2018),(1,70000,2019),(1,80000,2020),(2,60000,2018),(2,65000,2019),(2,65000,2020),(3,60000,2018),(3,65000,2019)]
df = spark.createDataFrame(data,["empid","salary","year"])
df.show()

lagdf=df.withColumn("incresalary",expr("salary - lag(salary) over (partition by empid order by year)")).fillna(0)
lagdf.show()
#------------------------------------------------------------------------------------------------------------
#Git Scenario 28:
#------------------------------------------------------------------------------------------------------------
#Git Scenario 29:
#------------------------------------------------------------------------------------------------------------
#Git Scenario 30:Write a SQL Query to extract second most salary for each department

data1 = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000),
]
df1 = spark.createDataFrame(data1, ["emp_id", "name", "dept_id", "salary"])
df1.show()

data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
df2.show()

df3=df1.join(df2,df1["dept_id"] == df2["dept_id1"],"inner")
df3.show()

sec_salary_df=df3.withColumn("sec_salary",expr(" dense_rank() over(partition by dept_id order by salary desc )"))
sec_salary_df.show()

final_result=sec_salary_df.filter("sec_salary == 2").select("emp_id", "name", "dept_name", "salary")
final_result.show()
#------------------------------------------------------------------------------------------------------------

#Git Scenario 31:Explode the dataframe.
from pyspark.sql.functions import *

data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
df.show()

con_df=df.selectExpr("concat(col1,'-', col2,'-', col3,'-', col4) as col ")
con_df.show()

flat_df=con_df.selectExpr(" explode(split(col,'-')) as col ")
flat_df.show()
#------------------------------------------------------------------------------------------------------------

# Git Scenario 32: Food Rating
from pyspark.sql.functions import *
data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]

df1 = spark.createDataFrame(data,["food_id","food_item"])
df1.show()

ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]

df2 = spark.createDataFrame(ratings,["food_id","rating"])
df2.show()

df3=df1.join(df2,df1["food_id"] == df2["food_id"],"inner").drop(df2["food_id"])
df3.show()

rating_df=df3.withColumn("stats(out of 5)",expr("repeat('*',rating)"))
rating_df.show()
#------------------------------------------------------------------------------------------------------------
# Git Scenario 33:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 35:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 36:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 37:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 38:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 39:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 40:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 41:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 42:
#------------------------------------------------------------------------------------------------------------
# Git Scenario 43: