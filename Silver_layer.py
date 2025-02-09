import json
import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pathlib import Path

# Set up logging
logging.basicConfig(filename="silver_layer_processing.log", level=logging.INFO)
logger = logging.getLogger()

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# Load Configurations
config_path = "config.json"
with open(config_path, "r") as config_file:
    config = json.load(config_file)

# Get paths and subdirectories from config
silver_input = Path(config["Silver_path"]["s_input_Path"])
silver_output = Path(config["Silver_path"]["s_Output_Path"])
sub_dirs = config["Bronze_paths"]["sub_dirs"]
today_date = datetime.datetime.today().strftime("%Y%m%d")

# Get list of already processed Silver dates
processed_silver = set(config.get("processed_silver", []))

logger.info(f"Starting Silver Layer processing for {today_date}")

# Check if today's Silver data has already been processed
if today_date in processed_silver:
    logger.info(f"Silver data for {today_date} has already been processed. Skipping...")
else:
    logger.info("Processing Silver layer...")

    dataframes = {}

    # Read data with schema inference
    for sub_dir in sub_dirs:
        path = silver_input / sub_dir / today_date / "*.csv"
        df = spark.read.csv(str(path), header=True, inferSchema=True)

        logger.info(f"Reading data from {path}")

        if df.isEmpty():
            logger.warning(f"No data found for {sub_dir}. Creating an empty DataFrame.")
            dataframes[sub_dir] = spark.createDataFrame([], StructType([]))  # Create an empty DataFrame
        else:
            dataframes[sub_dir] = df.dropDuplicates()

    # Get DataFrames (Handling missing data)
    df_orders = dataframes.get("raw_Orders", spark.createDataFrame([], StructType([])))
    df_customers = dataframes.get("raw_Customers", spark.createDataFrame([], StructType([])))
    df_order_items = dataframes.get("raw_Order_Items", spark.createDataFrame([], StructType([])))
    df_products = dataframes.get("raw_Products", spark.createDataFrame([], StructType([])))

    # Renaming 'source_system' and 'timestamp' columns with a suffix to prevent conflict during join
    df_order_items = df_order_items.withColumnRenamed("source_system", "source_system_order_items") \
        .withColumnRenamed("timestamp", "timestamp_order_items")
    df_orders = df_orders.withColumnRenamed("source_system", "source_system_orders") \
        .withColumnRenamed("timestamp", "timestamp_orders")
    df_products = df_products.withColumnRenamed("source_system", "source_system_products") \
        .withColumnRenamed("timestamp", "timestamp_products")
    df_customers = df_customers.withColumnRenamed("source_system", "source_system_customers") \
        .withColumnRenamed("timestamp", "timestamp_customers")

    # Perform Joins to Validate Data Relationships (with renamed columns)
    df_order_items_valid = df_order_items.join(df_orders, on="order_id", how="inner")
    df_orders_valid = df_orders.join(df_customers, on="customer_id", how="inner")
    df_products_valid = df_products.join(df_order_items, on="product_id", how="inner")
    df_customers_valid = df_customers.join(df_orders, on="customer_id", how="inner")

    # After joining, rename the columns back to their original names
    df_order_items_valid = df_order_items_valid.withColumnRenamed("source_system_order_items", "source_system") \
        .withColumnRenamed("timestamp_order_items", "timestamp")
    df_orders_valid = df_orders_valid.withColumnRenamed("source_system_orders", "source_system") \
        .withColumnRenamed("timestamp_orders", "timestamp")
    df_products_valid = df_products_valid.withColumnRenamed("source_system_products", "source_system") \
        .withColumnRenamed("timestamp_products", "timestamp")
    df_customers_valid = df_customers_valid.withColumnRenamed("source_system_customers", "source_system") \
        .withColumnRenamed("timestamp_customers", "timestamp")

    # Ensure Critical Fields Are NOT NULL
    df_order_items_valid = df_order_items_valid.filter(
        col("order_id").isNotNull() & col("product_id").isNotNull()
    )
    df_orders_valid = df_orders_valid.filter(col("order_id").isNotNull())
    df_products_valid = df_products_valid.filter(col("product_id").isNotNull())
    df_customers_valid = df_customers_valid.filter(col("customer_id").isNotNull())

    # Ensure Quantity & Price are Positive
    if "quantity" in df_order_items_valid.columns and "unit_price" in df_order_items_valid.columns:
        df_order_items_valid = df_order_items_valid.filter(
            (col("quantity") > 0) & (col("unit_price") > 0)
        )

    # Write Data to Silver Layer
    logger.info("Writing validated data to Silver Layer...")
    df_order_items_valid.write.mode("overwrite").option("header", True).csv(
        str(silver_output / "order_items_valid" / today_date)
    )
    df_orders_valid.write.mode("overwrite").option("header", True).csv(
        str(silver_output / "orders_valid" / today_date)
    )
    df_products_valid.write.mode("overwrite").option("header", True).csv(
        str(silver_output / "products_valid" / today_date)
    )
    df_customers_valid.write.mode("overwrite").option("header", True).csv(
        str(silver_output / "customers_valid" / today_date)
    )

    logger.info("Data successfully written to Silver Layer!")

    # Update processed_silver in config.json
    processed_silver.add(today_date)
    config["processed_silver"] = list(processed_silver)

    # Save the updated config.json
    with open(config_path, "w") as config_file:
        json.dump(config, config_file, indent=4)

    logger.info(f"Silver data for {today_date} has been processed and written to the Silver layer.")

print("Silver layer processing complete!")
