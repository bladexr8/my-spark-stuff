package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DDLSchemaExample {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("DDLSchemaExample")
            .getOrCreate()

        if (args.length <= 0) {
            println("usage: DDLSchemaExample <file path to blogs.json>")
            System.exit(1)
        }

        // Get the path to the JSON file
        val jsonFile = args(0)

        // Define our schema programmatically
        val schema = StructType(Array(StructField("Id", IntegerType, false),
            StructField("First", StringType, false),
            StructField("Last", StringType, false),
            StructField("Url", StringType, false),
            StructField("Published", StringType, false),
            StructField("Hits", IntegerType, false),
            StructField("Campaigns", ArrayType(StringType), false)))

            // Create a DataFrame by reading from the JSON file with a
            // pre-defined schema
            val blogsDF = spark.read.schema(schema).json(jsonFile)

            // Show the DataFrame as output
            blogsDF.show(false)

            // Print the schema
            println(blogsDF.printSchema)
            println(blogsDF.schema)
    }
}