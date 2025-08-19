# Databricks notebook source
type(spark)

# COMMAND ----------

from pyspark.sql.types import *
spark

# COMMAND ----------

# DBTITLE 1,Data Frame
from pyspark.sql.types import *
data = [(7, 'Yeshwin'), (7, 'Ronaldo')]
schema = StructType([StructField(name='id', dataType = IntegerType()), \
                     StructField(name='name', dataType = StringType())])
df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,read.csv()
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.csv('dbfs:/FileStore/tables/username.csv', header=True)
df.show(40)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,withColumn()
from pyspark.sql.functions import *
data = [(7, 'Yeshwin', 3000), (7, 'Ronaldo', 5000)]
schema = ['id', 'name', 'salary']
df = spark.createDataFrame(data,schema)
df1 = df.withColumn('Country', lit('India'))
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,withColumnRenamed()
from pyspark.sql.functions import *
data = [(7, 'Yeshwin', 3000), (7, 'Ronaldo', 5000)]
schema = ['id', 'name', 'salary']
df = spark.createDataFrame(data,schema)
df1 = df.withColumnRenamed('salary', 'salary_amount')
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,StructType() & StructField()
from pyspark.sql.types import *
data = [(7, ['Yeshwin','Velkur']), (7, ['Cristiano','Ronaldo'])]
StructName = StructType([StructField(name='FirstName', dataType=StringType()), \
                         StructField(name='Lastname', dataType=StringType())])
schema = StructType([StructField(name='id', dataType = IntegerType()), \
                     StructField(name='name', dataType = StructName)])
df = spark.createDataFrame(data,schema)
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,arraytype()
from pyspark.sql.types import *
data = [('abc',[1,2]), ('mno', [4,5]), ('xyz', [7,8])]
schema = StructType([StructField(name='id', dataType = StringType()), \
                     StructField(name='numbers', dataType = ArrayType(IntegerType()))])
df = spark.createDataFrame(data, schema)
df1= df.withColumn('numbers1', col('numbers')[0])
df1.show()
df1.printSchema()                 

# COMMAND ----------

# DBTITLE 1,explode() & array_contains()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', ['Python', 'SQL']), (2, 'Venkat', ['Pyspark', 'Java'])]
schema = ['id', 'name', 'skills']
df = spark.createDataFrame(data, schema)
df.show()
df.withColumn('skill', explode('skills')).show()
df.withColumn('has_pysparkskill', array_contains(df.skills, 'Pyspark')).show()


# COMMAND ----------

# DBTITLE 1,split()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', ('Python, SQL')), (2, 'Venkat', ('Pyspark, Java'))]
schema = ['id', 'name', 'skills']
df = spark.createDataFrame(data, schema)
df.show()
df.withColumn('skillsArray', split(col('skills'), ' ,')).show()

# COMMAND ----------

# DBTITLE 1,array()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'Python', 'SQL'), (2, 'Venkat', 'Pyspark', 'Java')]
schema = ['id', 'name', 'skill1', 'skill2']
df = spark.createDataFrame(data, schema)
df.show()
df.withColumn('skillsArray', array(df.skill1, df.skill2)).show()

# COMMAND ----------

# DBTITLE 1,mapType()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', {"hair":"black","eye":"black"})]
schema = ['name', 'prop']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,map_keys() & map_values()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', {"hair":"black","eye":"black"}), ('Venkat', {"hair":"black","eye":"black"})]
schema = StructType([StructField(name='name', dataType = StringType()), \
                     StructField(name='prop', dataType = MapType(StringType(), StringType()))])
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
df1 = df.select('name', 'prop', explode(df.prop))
df1.show(truncate=False)
df1.withColumn('keys', map_keys('prop')).show()
df1.withColumn('values', map_values('prop')).show()

# COMMAND ----------

# DBTITLE 1,when() & otherwise()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000), (2, 'Venkat', 'female', 4000), (3, 'abcd', '', 6000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df1 = df.select('id', 'name', when(df.gender=='male', 'm').when(df.gender=='female', 'f').otherwise('unkown').alias('gender'))
df.show()
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,asc(), desc(), cast(), like() Functions
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000), (2, 'Venkat', 'female', 4000), (3, 'abcd', '', 6000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df2= df.sort(df.name.asc())
df3= df.sort(df.name.desc())
df4= df.select(df.id,df.name,df.salary.cast('int'))
df5= df.filter(df.name.like('G%'))
df.show()
df2.show()
df3.show()
df4.show()
df4.printSchema()
df5.show()


# COMMAND ----------

# DBTITLE 1,where(), filter()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000), (2, 'Venkat', 'female', 4000), (3, 'abcd', 'male', 6000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df1 = df.filter(df.gender == 'male')
df2 = df.where("salary  == '4000'")
df.show()
df1.show()
df2.show()

# COMMAND ----------

# DBTITLE 1,distinct(), dropDuplicates()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000), (2, 'Venkat', 'female', 4000), (2, 'Venkat', 'female', 4000), (3, 'abcd', 'male', 6000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df1=df.distinct()
df2=df.dropDuplicates(['gender'])
df.show()
df1.show()
df2.show()

# COMMAND ----------

# DBTITLE 1,sort() & orderBy()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Devi', 'female', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR'), (4, 'Sanjana', 'female', 6000, 'IT')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df1= df.sort(df.dep)
df2=df.sort(df.dep.desc())
df3=df.orderBy(df.name,df.salary.desc())
df.show()
df1.show()
df2.show()
df3.show()

# COMMAND ----------

# DBTITLE 1,groupBy() Function
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Devi', 'female', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR'), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, 'Sumanth', 'male', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.show()
df1=df.groupBy('dep').count().show()
df2=df.groupBy('dep').min('salary').alias('minsal').show()
df3=df.groupBy('dep').max('salary').alias('maxsal').show()
df4=df.groupBy('dep', 'gender').count().show()
df5= df.groupBy('dep').agg(count('*').alias('Count of Emps'),min('salary').alias('minsal'),max('salary').alias('maxsal')).show()

# COMMAND ----------

# DBTITLE 1,union() & unionAll()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data1 = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Devi', 'female', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR')]
data2 =[(4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, 'Sumanth', 'male', 3000, 'HR')]
schema1 = ['id', 'name', 'gender', 'salary', 'dep']
schema2 = ['id', 'name', 'gender', 'salary', 'dep']
df1 = spark.createDataFrame(data1, schema)
df2 = spark.createDataFrame(data2, schema)
df1.show()
df2.show()
df3=df1.union(df2).show() 
df4=df1.unionAll(df2).show()

# COMMAND ----------

# DBTITLE 1,unionByName()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data1 = [(1, 'Gowtham', 3000,'IT'), (2, 'Devi', 4000, 'HR'), (3, 'Bhaskar', 4000, 'HR')]
data2 =[(4, 'Sanjana', 'female', 6000), (5, 'Grishma', 'female', 6000), (6, 'Sumanth', 'male', 3000)]
schema1 = ['id', 'name', 'salary', 'dep']
schema2 = ['id', 'name', 'gender', 'salary']
df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)
df3=df1.unionByName(df2, allowMissingColumns=True)
df1.show()
df2.show()
df3.show()

# COMMAND ----------

# DBTITLE 1,select()


data = [(1, 'Gowtham', 'male', 3000)]
schema = ['id', 'name', 'gender', 'salary']

df = spark.createDataFrame(data, schema)
df1 = df.select('id', 'name').show()
df2 = df.select(df.id, df.name).show()
df3 = df.select(col('id'), col('name')).show()
df4 = df.select(df['id'], df['name']).show()
df5 = df.select(['id', 'name']).show()
df6 = df.select('*').show()

# COMMAND ----------

# DBTITLE 1,join() Functions
from pyspark.sql.functions import *
from pyspark.sql.types import *
data1 = [(1, 'Gowtham', 3000, 1), (2, 'Grishma', 6000, 4), (3, 'Venkat', 4000, 2)]
data2 = [(1, 'IT'), (2,'HR'), (3,'MR')]
schema1 = ['id', 'name', 'salary', 'dep']
schema2=['id', 'dep_name']
df1= spark.createDataFrame(data1, schema1)
df2=spark.createDataFrame(data2, schema2)
df3= df1.join(df2, df1.dep == df2.id, 'inner')
df4=df1.join(df2, df1.dep == df2.id, 'left')
df5= df1.join(df2, df1.dep == df2.id, 'right')
df6=df1.join(df2, df1.dep == df2.id, 'full')
df7=df1.join(df2, df1.dep == df2.id, 'leftsemi')
df8=df1.join(df2, df1.dep == df2.id, 'leftanti')
df1.show()
df2.show()
df3.show()
df4.show()
df5.show()
df6.show()
df7.show()
df8.show()


# COMMAND ----------

# DBTITLE 1,self join()


data= [(1, "gowtham", 0), (2, "Grishma", 1), (3, "Venkat", 2)]
schema = ['id', 'name', 'managerID']

df= spark.createDataFrame(data, schema)
df1=df.alias('empData').join(df.alias('MgrData'), col('empData.managerId') == col('MgrData.id'), 'inner')
df1.select(col('empData.name'),col('MgrData.name')).show()
df1.show()

# COMMAND ----------

# DBTITLE 1,pivot()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Devi', 'female', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR'), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, 'Sumanth', 'male', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df1 = df.groupBy('dep').pivot('gender').count()
df2 = df.groupBy('dep').pivot('gender',['male']).count()
df.show()
df1.show()
df2.show()

# COMMAND ----------

# DBTITLE 1,unpivot using stack()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('IT', 10, 7), ('HR', 11, 8), ('MR', 13, 9)]
schema = ['dep', 'male', 'female']
df = spark.createDataFrame(data, schema)
df1 = df.select('dep', expr("stack(2, 'M', male, 'F', female) as (gender, count)"))
df.show()
df1.show()


# COMMAND ----------

# DBTITLE 1,fill() & fillna()
from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'abcd', None, 4000, 'HR'), (3, 'efgh', 'male', 4000, None), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, None, 'male', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.show()
df.fillna('unkown').show()
df.na.fill('unkown', ['dep']).show()

# COMMAND ----------

# DBTITLE 1,sample()
df = spark.range(start = 1, end = 101)
df1= df.sample(fraction = 0.1, seed=9)
df.show()
df1.show()

# COMMAND ----------

# DBTITLE 1,collect()
data = [(1, 'Gowtham', 3000), (2, 'Venkat', 4000)]
schema = ['id', 'name', 'salary']
df=spark.createDataFrame(data,schema)
listrows=df.collect()
print(listrows)
print(listrows[0])
print(listrows[0][1])

# COMMAND ----------

# DBTITLE 1,transform()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [(1, 'Gowtham', 3000), (2, 'Venkat', 4000)]
schema = ['id', 'name', 'salary']
df=spark.createDataFrame(data,schema)
def ConvertToUpperCase(df):
    return df.withColumn('name', upper(df.name))
def TripleSalary(df):
    return df.withColumn('salary', df.salary*3)
df1= df.transform(ConvertToUpperCase).transform(TripleSalary)     
df1.show()   

# COMMAND ----------

# DBTITLE 1,pyspark.sql.functions.transform()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [(1, 'Gowtham', ['Python', 'SQL']), (2, 'Venkat', ['Pyspark', 'Tableau'])]
schema = ['id', 'name', 'skills']
df = spark.createDataFrame(data,schema)
df1 = df.select('id', 'name', transform('skills', lambda x : upper(x)).alias('skills'))

def convertToUpper(x):
    return upper(x)

df2 = df.select('id', 'name', transform('skills', lambda x : convertToUpper(x)).alias('skills')) 
df.show()
df1.show()
df2.show()

# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [(1, 'Gowtham', 3000), (2, 'Venkat', 4000)]
schema = ['id', 'name', 'salary']
df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView('emp')
df1 = spark.sql('SELECT id,upper(name) as name FROM emp')
df1.show()



# COMMAND ----------

# DBTITLE 1,createOrReplaceGlobalTempView()


data = [(1, 'Gowtham', 3000), (2, 'Venkat', 4000)]
schema = ['id', 'name', 'salary']
df = spark.createDataFrame(data, schema)
df.createOrReplaceGlobalTempView('empGlobal')
df1 = spark.sql('SELECT id,upper(name) as name FROM global_temp.empGlobal')
df1.show()

# COMMAND ----------

# DBTITLE 1,listing temp tables and global temp tables
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark.catalog.listTables('default')
spark.catalog.listTables('global_temp')


# COMMAND ----------

# DBTITLE 1,dropping temp tables and global temp tables
spark.catalog.dropTempView('emp')
spark.catalog.dropGlobalTempView('empGlobal')

# COMMAND ----------

# DBTITLE 1,udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [(1, 'Gowtham', 3000, 1000), (2, 'Venkat', 4000, 1000)]
schema = ['id', 'name', 'salary', 'bonus']
df = spark.createDataFrame(data, schema)
def totalpay(x,y):
    return x+y
TotalPay = udf(lambda x,y: totalpay(x,y), IntegerType())
df1= df.withColumn('TotalSalary', TotalPay(df.salary, df.bonus))

@udf(returnType = IntegerType())
def totalpay(x,y):
    return x+y
df2 = df.withColumn('Totalsalary', totalpay(df.salary, df.bonus))   
df.show() 
df1.show()
df2.show()

# COMMAND ----------

# DBTITLE 1,rdd to df
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [(1, 'Gowtham'), (2, 'Venkat')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

df1 = rdd.toDF(schema = ['id', 'name'])
df2 = spark.createDataFrame(rdd, schema = ['id', 'name'])
df1.show()
df2.show()


# COMMAND ----------

# DBTITLE 1,map() in rdd
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', 'Venkat'), ('Grishma', 'Tallapareddy')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect)
rdd1 = rdd.map(lambda x: x + (x[0] + ' ' + x[1],))
print(rdd1.collect())

df = spark.createDataFrame(data, ['Fn', 'Ln'])
rdd = df.rdd.map(lambda x: x + (x[0] + ' ' + x[1],))
df1 = rdd.toDF(['Fn', 'Ln', 'FullName'])
df1.show()

def fullname(x):
    return x + (x[0] + ' ' + x[1],)
rdd2 = df.rdd.map(lambda x: fullname(x))
print(rdd2.collect())


# COMMAND ----------

# DBTITLE 1,rdd flatMap()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = ['Gowtham Venkat', 'Grishma Tallapareddy']
rdd = spark.sparkContext.parallelize(data)
for i in rdd.collect():
    print(i)

rdd1 = rdd.map(lambda x: x.split(' '))
for item in rdd1.collect():
    print(item)

rdd2 = rdd.flatMap(lambda x: x.split(' '))
for item in rdd2.collect():
    print(item)
    

# COMMAND ----------

# DBTITLE 1,partitionBy()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Devi', 'female', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR'), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, 'Sumanth', 'male', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.write.parquet("dbfs:/FileStore/employee1", partitionBy='dep')
df.write.parquet("dbfs:/FileStore/employee2", partitionBy=['dep', 'gender'])

# COMMAND ----------

# DBTITLE 1,from_json()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', '{"hair":"black","eye":"black"}')]
schema = ['name', 'prop']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
MapTypeSchema = MapType(StringType(), StringType())
df1= df.withColumn("propsMap", from_json('prop', MapTypeSchema))
df1.show(truncate=False)
df1.printSchema()
StructTypeSchema = StructType([StructField('hair', StringType()),
                                StructField('eye', StringType())])           
df2=df1.withColumn('propsStruct', from_json('prop', StructTypeSchema))
df2.show(truncate=False) 
df2.printSchema()                               

# COMMAND ----------

# DBTITLE 1,to_json MapType()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', {"hair":"black","eye":"black"})]
schema = ['name', 'prop']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
df1=df.withColumn('propMap', to_json('prop'))
df1.show(truncate=False)
df1.printSchema

# COMMAND ----------

# DBTITLE 1,to_json() StructType()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', ('black', 'brown'))]
schema = StructType([StructField('name', StringType()),
                    StructField('prop', StructType([StructField('hair', StringType()),StructField('eye', StringType())]))]) 
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
df1 = df.withColumn('propStruct', to_json('prop'))
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,json_tuple()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', '{"hair":"black","eye":"black","skin":brown"}'), ('Grishma', '{"hair":"black","eye":"brown","skin":white"}')]
schema = ['name', 'prop']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
df1 = df.select('name', json_tuple(df.prop ,'hair','eye'))
df1.show()


# COMMAND ----------

# DBTITLE 1,get_json_object()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('Gowtham', '{"adress":{"state":"Telangana","city":"hyderabad"},"gender":"male"}'), ('Grishma', '{"adress":{"state":"Andhra Pradesh","city":"Vijayawada"},"eye":"black"}')]
schema = ['name', 'prop']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()
df.select("name", get_json_object('prop', '$.adress.state').alias('state'),  get_json_object('prop', '$.adress.city').alias('city')).show()

# COMMAND ----------

# DBTITLE 1,date functions()
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.range(3)
df.show()
df1 = df.withColumn('TodayDate', current_date())
df1.show()
df2 = df1.withColumn('TodayDate', date_format(df1.TodayDate, 'MM/dd/yyyy'))
df2.show()
df3=df2.withColumn('TodayDate', to_date(df2.TodayDate, 'MM/dd/yyyy'))
df3.show()

# COMMAND ----------

# DBTITLE 1,date functions()
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [('2024-11-15', '2024-09-15')]
schema = ['d1', 'd2']
df = spark.createDataFrame(data, schema)
df.show()
df.withColumn('datedifference', datediff('d1','d2')).show()
df.withColumn('monthsbetween', months_between('d1','d2')).show()
df.withColumn('addmonths', add_months('d1',6)).show()
df.withColumn('subtractmonths', add_months('d1',-6)).show()
df.withColumn('adddays', date_add('d1',180)).show()
df.withColumn('subtractdays', date_add('d1',-180)).show()
df.withColumn('year', year('d1')).show()
df.withColumn('month', month('d1')).show()






# COMMAND ----------

# DBTITLE 1,timestamp functions()
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.range(3)
df.show()
df1 = df.withColumn('currentTimestamp', current_timestamp())
df1.show(truncate=False)
df2 = df1.select('*', hour(df1.currentTimestamp).alias('hour'), minute(df1.currentTimestamp).alias('minute'), second(df1.currentTimestamp).alias('second'))
df2.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Venkat', 'male', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR'), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, 'Devi', 'female', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.show()
df.select(approx_count_distinct('dep')).show()
df.select(avg('salary')).show()
df.select(collect_list('dep')).show(truncate=False)
df.select(collect_set('dep')).show()
df.select(count('dep')).show()



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'Venkat', 'male', 4000, 'HR'), (3, 'Bhaskar', 'male', 4000, 'HR'), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, 'Devi', 'female', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.show()
window = Window.partitionBy('dep').orderBy('salary')
df.withColumn('rownumber', row_number().over(window)).show()
df.withColumn('rank', rank().over(window)).show()
df.withColumn('denserank', dense_rank().over(window)).show()
