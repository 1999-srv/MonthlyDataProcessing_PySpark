# Sales Data Processing by Year and Month using PySpark
This PySpark script loads sales data, extracts the year and month from the OrderDate column, and processes the data by each year and month. The script then filters and displays the data for each distinct month in the dataset.

# Requirements
Apache Spark with PySpark installed.

Sales data stored in a CSV file with columns such as SalesOrderID, OrderQty, ProductID, UnitPrice, and OrderDate.

# Script Overview
**Step 1**: Define Schema
The schema for the CSV file is defined using PySpark's StructType to ensure proper data types for each column, including converting the OrderDate column to a DateType.

    schema = StructType([
        StructField("SalesOrderID", IntegerType(), True),
        StructField("SalesOrderDetailID", IntegerType(), True),
        ...
        StructField("OrderDate", DateType(), True)
    ])

**Step 2**: Load the CSV Data
The data is loaded from a CSV file using the defined schema. The OrderDate is cast to a timestamp for further processing.
    
    df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")
    df = df.withColumn("OrderDate", df["OrderDate"].cast("timestamp"))

**Step 3**: Extract Year and Month
The script extracts the year and month from the OrderDate column using PySpark functions and adds them as new columns (Year, Month).
    
    df = df.withColumn("Year", year(df["OrderDate"]))
    df = df.withColumn("Month", month(df["OrderDate"]))

**Step 4**: Filter Data by Month
Using the distinct year and month values, the script iterates through the data and filters records for each specific month. It stores the filtered data in a dictionary and displays the first 5 rows for each month.

    distinct_months = df.select("Year", "Month").distinct().orderBy("Year", "Month").collect()
    for row in distinct_months:
        year_val = row['Year']
        month_val = row['Month']
        # Filter data for the specific year and month
        monthly_df = df.filter((df["Year"] == year_val) & (df["Month"] == month_val))
        # Print and display the data
        print(f"Data for {year_val}-{month_val:02d}:")
        monthly_df.show(5)
# Output
The script outputs the first 5 rows of the dataset for each unique year and month. The output displays the filtered records for each month in the format YYYY-MM.

# Usage Instructions
Place the sales data CSV file at the specified location: /FileStore/tables/Sales_SalesOrderDetail.csv.

Run the script to load and process the data.

The script will print and display the records for each year and month combination.

# File Structure
**Data Source**: /FileStore/tables/Sales_SalesOrderDetail.csv
**Columns**:
SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber, OrderQty, ProductID, UnitPrice, UnitPriceDiscount, LineTotal, OrderDate.
