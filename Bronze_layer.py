import json
import datetime
import platform
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pathlib import Path

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

# Load Config
config_path = "config.json"
with open(config_path, "r") as config_file:
    config = json.load(config_file)

# Paths from Config
bronze_paths = config["Bronze_paths"]
input_folder = Path(bronze_paths["input_Path"])
sub_dirs = bronze_paths["sub_dirs"]
output_path = Path(bronze_paths["B_output_folder"])

# Get today's date in YYYYMMDD format
today_date = datetime.datetime.today().strftime("%Y%m%d")

# Get list of already processed Bronze dates
processed_bronze = set(config.get("processed_bronze", []))

# Check if today's Bronze data has already been processed
if today_date in processed_bronze:
    print(f"Bronze data for {today_date} has already been processed. Skipping...")
else:
    # Process data if not already processed
    for sub_dir in sub_dirs:
        dynamic_path = input_folder / sub_dir / today_date / "*.csv"  # Construct path
        print(f"Reading from: {dynamic_path}")

        # Read data from Landing Zone
        df = spark.read.csv(str(dynamic_path), header=True, inferSchema=True)
        df.show()

        # Add metadata columns
        source = platform.node()
        df = df.withColumn("timestamp", current_timestamp())
        df = df.withColumn("Source_system", lit(source))

        # Output path for writing CSVs
        dynamic_output_path = output_path / sub_dir / today_date  # Maintain subdirectory structure
        print(f"Writing to: {dynamic_output_path}")

        # Write data to the output path
        df.write.mode("overwrite").option("header", True).csv(str(dynamic_output_path))

    # Update processed_bronze in config.json
    processed_bronze.add(today_date)
    config["processed_bronze"] = list(processed_bronze)

    # Save the updated config.json
    with open(config_path, "w") as config_file:
        json.dump(config, config_file, indent=4)

    print(f"Bronze data for {today_date} has been processed and written to {output_path}")

print("Bronze layer processing complete!")
