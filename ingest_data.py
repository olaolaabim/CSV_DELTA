from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
import os
import uuid



def main():
    spark = SparkSession.builder.appName("CSV_TO_DELTA").getOrCreate()
    #The file path
    #csv_path = 'C://Users//44776//Desktop//CFpartners//codetask//input'
    csv_dir = '/app/input'
    delta_lake_dir = '/app/output/'

    # Define Schema
    sales_schema = StructType([
        StructField("itemID", StringType(), True),
        StructField("customerID", StringType(), True),
        StructField("itemName", StringType(), True),
        StructField("itemAmount", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
    ])
    # list all the CSV files in the directory
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

    # initializing the dataframe to null
    df_sales = None

    # Loop through the csv Files
    for file in csv_files:
        file_path = os.path.join(csv_dir, file)
        try:
            # Read CSV file with header
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
            # If CSV file has no header, use defined schema
            if "item_id" not in df.columns:
                df = spark.read.option("header", "false").schema(sales_schema).csv(file_path)
            # Concatenate dataframe to df_sales
            if df_sales is None:
                df_sales = df
            else:
                df_sales = df_sales.union(df)
        except Exception as e:
            print(f"Error reading file {file}: {e}")
    
    # Add ingestion_tms and batch_id columns to DataFrame
    df_sales_timed = df_sales.withColumn("ingestion_tms", current_timestamp().cast(StringType()))
    df_sales_batched = df_sales_timed.withColumn("batch_id", lit(str(uuid.uuid4())))
    df_sales_batched.show()

    # Write batched dataframe to DeltaLake using append
    try:
        df_sales_batched.write.mode("append").format("delta").save(delta_lake_dir)
        print(f"Successfully wrote {df_sales_batched.count()} rows")
    except Exception as e:
        print(f"Error writing to DeltaLake: {e}")


if __name__ == '__main__':

    main()