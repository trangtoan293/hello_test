
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._


// Read CSV file with defined schema
val schemaDF = spark.read 
  .csv("path_to_your_schema_csv_file.csv")

// Convert schema DataFrame to Map
val schemaMap = schemaDF.collect().map(row => (row.getString(0), row.getString(1))).toMap

// Create a new schema for reading data file
val dataSchema = StructType(
  schemaMap.map { case (columnName, dataType) =>
    StructField(columnName, dataType match {
      case "string" => StringType
      case "integer" => IntegerType
      case "timestamp" => TimestampType
      // Add more data types as needed
      case _ => StringType // Default to StringType
    }, nullable = true)
  }.toList
)

// Read data file using the custom schema
val dataDF = spark.read
  .schema(dataSchema)
  .option("header", "true")
  .csv("path_to_your_data_csv_file.csv")

// Perform operations on dataDF as needed

// Show the DataFrame
dataDF.show()
dataDF.createOrReplaceTempView("test_data")


   