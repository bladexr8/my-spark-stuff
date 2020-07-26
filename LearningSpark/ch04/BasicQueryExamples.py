from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create a SparkSession
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Path to dataset
csv_file = "data/departuredelays.csv"

# Read and create a temporary view
# Infer schema (for larger files
# # you may want to specify the schema)
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

# find flights whose distance is > 1000 miles
print("\n***Flights > 1000 miles***\n")
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl
WHERE distance > 1000
ORDER BY distance DESC""").show(10)

# flights b/w San Francisco (SFO) and Chicago (ORD)
# with at least a 2 hour delay
print("\n***SFO -> ORD Flights min 2 hour delay***\n")
spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER BY delay DESC""").show(10)

# classify delays
# SQL statement is a pain in the butt...
# doesn't work with any more conditions added!
print("\n***Classified Delays***\n")
# spark.sql("""SELECT delay, origin, destination, 
# CASE 
# WHEN delay > 360 THEN 'Very Long Delays' WHEN delay > 120 AND delay < 360 THEN 'Long Delays'  
# ELSE 'Not too Bad' 
# END AS Flight_Delays
# FROM us_delay_flights_tbl
# ORDER BY origin, delay DESC""").show(10)

# WHEN delay > 120 AND < 360 THEN 'Long Delays'
# WHEN delay > 60 AND < 120 THEN 'Short Delays'
# WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'

# re-write using DataFrame API
caseDF = df.withColumn("Flight_Delays", when(col("delay") > 360, "Very Long Delays")
.when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
.when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
.when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
.otherwise("Early"))

# use sort() instead of orderBy() due to better control over sorting 
# when using multiple columns
(caseDF
    .select("delay", "origin", "destination", "Flight_Delays")
    .sort(asc("origin"), desc("delay"))
    .show(10))
