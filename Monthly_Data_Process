from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import year, month

# Define the schema
schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitPriceDiscount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(20, 2), True),
    StructField("rowguid", StringType(), True),
    StructField("OrderDate", DateType(), True)
])

# Load the data with the schema
df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

# Show the first few rows to verify the data
# df.show(5)

# Convert the 'OrderDate' column to a proper timestamp type
df = df.withColumn("OrderDate", df["OrderDate"].cast("timestamp"))

# Extract year and month from the 'OrderDate' column
df = df.withColumn("Year", year(df["OrderDate"]))
df = df.withColumn("Month", month(df["OrderDate"]))

# Get distinct year and month values
distinct_months = df.select("Year", "Month").distinct().orderBy("Year", "Month").collect()

# Create a dictionary to store DataFrames for each month
month_dataframes = {}

# Iterate through the distinct year and month combinations
for row in distinct_months:
    year_val = row['Year']
    month_val = row['Month']
    
    # Filter the data for the specific year and month
    monthly_df = df.filter((df["Year"] == year_val) & (df["Month"] == month_val))
    
    # Print the label for the current month
    print(f"Data for {year_val}-{month_val:02d}:")
    
    # Show the DataFrame for the current month
    monthly_df.show(5)
