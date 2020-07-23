from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a SparkSession
spark = (SparkSession
    .builder
    .appName("sf_fire")
    .getOrCreate())

# Programmatic way to define a schema
fire_schema = StructType(
    [
        StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', IntegerType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),
        StructField('Delay', FloatType(), True)
    ]
)

sf_fire_file = "data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# save as a Parquet file
# parquet_path = "data/sf-fire-calls-parquet"
# fire_df.write.format("parquet").save(parquet_path)

# a "projection" is a way to return only the rows matching a 
# certain relational condition by using filters
# in Spark projections are done with the "select" method
# filters can be expressed using the "filter" method
print("\n***projection***\n")
few_fire_df = (fire_df
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# return number of distinct types of calls using countDistinct()
print("\n***distinct call types***\n")
(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))
    .show())

(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .distinct()
    .show(10, False))

# rename a column
print("\n***renamed column***\n")
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedInMins")
(new_fire_df
    .select("ResponseDelayedInMins")
    .where(col("ResponseDelayedInMins") > 5)
    .show(5, False))

# convert string columns to DateTime
print("\n***convert columns to DateTime***\n")
fire_ts_df = (new_fire_df
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "dd/MM/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "dd/MM/yyyy"))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "dd/MM/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm"))

# select the converted columns
(fire_ts_df
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, False))

# years incidents have occurred
print("\n***years incidents have occurred***\n")
(fire_ts_df
    .select(year("IncidentDate"))
    .distinct()
    .orderBy(year("IncidentDate"))
    .show())

# most common types of fire calls
print("\n***most common types of calls***\n")
(fire_ts_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(n=10, truncate=False))

print("\n***# alarms, avg response time, min & max response times***\n")
(fire_ts_df
    .select(sum("NumAlarms"), avg("ResponseDelayedInMins"), min("ResponseDelayedInMins"), max("ResponseDelayedInMins"))
    .show())
