from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# define schema for our data using DDL
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, \
            `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

# Create static data
data = [
    [1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
    [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
    [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
    [4, "Tathagata", "Das", "https://tinyurl.4", "2/12/2018", 10568, ["twitter", "FB"]],
    [5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
    [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
]

# Main Program
if __name__ == "__main__":
    # Create a SparkSession
    spark = (SparkSession
        .builder
        .appName("DDLSchemaExample")
        .getOrCreate())

    # Create a DataFrame using the schema defined above
    blogs_df = spark.createDataFrame(data, schema)
    # Show the DataFrame
    blogs_df.show()
    # print the schema used by Spark to process the DataFrame
    print(blogs_df.printSchema())

    print("\n***Big Hitters***\n")
    
    # computed column
    blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    print("\n***Concatenated Column***\n")
    blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(col("AuthorsId")).show(5)

    # column selection statements
    # these statements return the same value,
    # showing that expr is the same as a col
    # method call
    print("\n***expr***\n")
    blogs_df.select(expr("Hits")).show(2)

    print("\n***col***\n")
    blogs_df.select(col("Hits")).show(2)

    print("\n***select***\n")
    blogs_df.select("Hits").show(2)

    # Sort by column "Id" in descending order
    # "col" is an explicit  to return a Column object
    # "$" is a function in Spark that converts column Named Id to a Column
    # print("\n***select with col***\n")
    # blogs_df.sort(col("Id").desc).show()
    # print("\n***select with $***\n")
    # blogs_df.sort($"Id".desc).show()
