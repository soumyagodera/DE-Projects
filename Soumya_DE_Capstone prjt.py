# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.Create a DF(airlines_1987_to_2008) from this path
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/
# MAGIC  (There are csv files in airlines folder. It contains 1987.csv to  2008.csv files. 
# MAGIC Create only one DF from all the files )

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df1 = (spark.read\
  .format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("dbfs:/databricks-datasets/asa/airlines/")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ####2.Return count of records in dataframe

# COMMAND ----------

df1.count() 

# COMMAND ----------

df1.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.View the dataframe

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####4.Create a PySpark Datatypes schema for the above DF

# COMMAND ----------

schema=StructType([
StructField("Year",IntegerType(),True),
StructField("Month",IntegerType()),
StructField("DayofMonth", IntegerType()),
StructField("DayOfWeek", IntegerType()),
StructField("DepTime", DoubleType()),
StructField("CRSDepTime", DoubleType()),
StructField("ArrTime", DoubleType()),
StructField("CRSArrTime" ,DoubleType()),
StructField("UniqueCarrier", StringType()), 
StructField("FlightNum", StringType()),
StructField("TailNum", StringType()),
StructField("ActualElapsedTime",DoubleType()),
StructField("CRSElapsedTime",DoubleType()),
StructField("AirTime", StringType()),
StructField("ArrDelay",DoubleType()),
StructField("DepDelay",DoubleType()),
StructField("Origin", StringType()),
StructField("Dest", StringType()),
StructField("Distance",DoubleType()),
StructField("TaxiIn", StringType()),
StructField("TaxiOut", StringType()),
StructField("Cancelled",StringType()),
StructField("CancellationCode", StringType()),
StructField("Diverted", StringType()),
StructField("CarrierDelay", StringType()),
StructField("WeatherDelay", StringType()),
StructField("NASDelay", StringType()),
StructField("SecurityDelay",StringType()),
StructField("LateAircraftDelay",StringType())
            ])


# COMMAND ----------

#another way to create schema,Example
schema=([
("Year",IntegerType(),True),
("Month",IntegerType()),
("DayofMonth", IntegerType()),
("DayOfWeek", IntegerType()),
    ("DepTime", DoubleType())
schema=StructType([StructField(x[0],x[1],True)for x in schema])

# COMMAND ----------

# Read all the CSV files and create a DataFrame
path = "/databricks-datasets/asa/airlines/"
file_list = [f.path for f in dbutils.fs.ls(path) if f.path.endswith(".csv")]
df1 = spark.read.csv(file_list, header=True, schema=schema)


# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.Select the columns - Origin, Dest and Distance

# COMMAND ----------

df1.select("Origin","Dest","Distance").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6.Filtering data with 'where' method, where Year = 2001

# COMMAND ----------

filtered_data=df1.filter(df1.Year==2001)
#filtered_data.show()


# COMMAND ----------

display(filtered_data)

# COMMAND ----------

# MAGIC  %md
# MAGIC ####7.Create a new dataframe (airlines_1987_to_2008_drop_DayofMonth) exluding dropped column (“DayofMonth”) 

# COMMAND ----------

airlines_1987_to_2008_drop_DayofMonth = df1.drop("DayOfMonth")
airlines_1987_to_2008_drop_DayofMonth.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####8.Display new DataFrame

# COMMAND ----------

display(airlines_1987_to_2008_drop_DayofMonth)

# COMMAND ----------

9.# Create the 'Weekend' column based on the 'DayOfWeek' column
df_with_weekend = df1.withColumn("Weekend", when(col("DayOfWeek").isin(6, 7), "Yes").otherwise("No"))

# COMMAND ----------

# Create a new DataFrame 'AddNewColumn' with the added column
AddNewColumn= df_with_weekend.select("*")


# COMMAND ----------

display(AddNewColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC ####10.Cast ActualElapsedTime column to integer and use printschema to verify

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# Cast 'ActualElapsedTime' column to IntegerType
CastColumn = df1.withColumn("ActualElapsedTime",df1["ActualElapsedTime"].cast(IntegerType()))

# COMMAND ----------

CastColumn.printSchema()# ActualElapsedTime is double changed to integer

# COMMAND ----------

# MAGIC %md
# MAGIC ####11.Rename 'DepTime' to 'DepartureTime

# COMMAND ----------

# Rename 'DepTime' column to 'DepartureTime'
RenamedColumn = df1.withColumnRenamed("DepTime", "DepartureTime")

# COMMAND ----------

RenamedColumn.printSchema()

# COMMAND ----------

display(RenamedColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC ####12:Drop duplicate rows based on Year and Month and Create new df (Drop Rows)

# COMMAND ----------

DropRows=df1.dropDuplicates(['Year','Month'])

# COMMAND ----------

display(DropRows)

# COMMAND ----------

# MAGIC %md
# MAGIC ####13.Display Sort by descending order for Year Column using sort()

# COMMAND ----------

Sorted_df=DropRows.sort(DropRows['Year'].desc())

# COMMAND ----------

display(Sorted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####14.Group data according to Origin and returning count

# COMMAND ----------

grouped_df=DropRows.groupBy("Origin").count()

# COMMAND ----------

display(grouped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####15.Group data according to dest and finding maximum value for each 'Dest'

# COMMAND ----------

group_df = RenamedColumn.groupBy('Dest').max('DepartureTime')

# COMMAND ----------

display(group_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####16.Write data in Delta format

# COMMAND ----------

# Write DataFrame in Delta format
DropRows.write.format("delta").mode("overwrite").save("/dbfs:/databricks-datasets/asa/airlines/DeltaAirlines")

# COMMAND ----------

#To check :%fs ls dbfs:/databricks-datasets/asa/airlines/
