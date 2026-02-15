import pyspark
from pyspark.sql import SparkSession

print("Starting Spark Session...")

# Create Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test_prajwal') \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# Create a small test dataframe
data = [("Prajwal", 1), ("DataTalks", 2), ("Zoomcamp", 3)]
df = spark.createDataFrame(data, ["Name", "ID"])

# Show the results
df.show()

print("Verification Successful!")

# Stop the session
spark.stop()