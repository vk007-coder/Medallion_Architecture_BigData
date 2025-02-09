from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, sum, count, abs
from pathlib import Path
import json
from datetime import datetime
import os

# Load Configurations
config_path = "config.json"
config = json.load(open(config_path))

gold_output = Path(config["Gold_Paths"]["g_output_Path"])
silver_input = Path(config["Gold_Paths"]["g_input_Path"])
today_date = datetime.now().strftime("%Y%m%d")

processed_gold = set(config.get("processed_gold", []))

if today_date in processed_gold:
    print(f"Data for {today_date} already processed. Skipping...")
else:
    gold_layer_path = gold_output / today_date
    os.makedirs(gold_layer_path, exist_ok=True)

    spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

    def read_csv_check(path):
        full_path = silver_input / path / today_date
        if not full_path.exists():
            print(f"Warning: Missing {path}. Skipping.")
            return None
        df = spark.read.csv(str(full_path / "*.csv"), header=True, inferSchema=True)
        if df.isEmpty():
            print(f"Warning: Empty data for {path}. Skipping.")
            return None
        return df

    df_orders_valid = read_csv_check("orders_valid")
    df_order_items_valid = read_csv_check("order_items_valid")
    df_products_valid = read_csv_check("products_valid")
    df_customers_valid = read_csv_check("customers_valid")

    if None in [df_orders_valid, df_order_items_valid, df_products_valid, df_customers_valid]:
        print("Missing required datasets. Stopping process.")
    else:
        # Trim any spaces in order_id columns
        df_orders_valid = df_orders_valid.withColumn("order_id", trim(col("order_id")))
        df_order_items_valid = df_order_items_valid.withColumn("order_id", trim(col("order_id")))

        # Filter out cancelled orders
        df_orders_valid = df_orders_valid.filter(col("order_status") != "CANCELLED")

        # Perform joins and aggregate sales data
        df_sales = df_orders_valid.alias("o") \
            .join(df_order_items_valid.alias("oi"), "order_id", "left") \
            .join(df_products_valid.alias("p"), "product_id", "left") \
            .join(df_customers_valid.alias("c"), "customer_id", "left") \
            .select("o.order_id", "c.customer_id", "c.address", "oi.unit_price", "oi.quantity", "oi.product_id")

        df_sales_agg = df_sales.groupBy("order_id", "customer_id", "address") \
            .agg(sum(col("unit_price") * col("quantity")).alias("total_revenue"), count("product_id").alias("total_items"))

        df_order_totals = df_orders_valid.groupBy("order_id").agg(sum("total_amount").alias("order_total_amount"))

        df_sales_agg_with_totals = df_sales_agg.join(df_order_totals, "order_id", "left") \
            .withColumn("revenue_check", abs(col("total_revenue") - col("order_total_amount")) < 0.1)

        # Show revenue_check for inspection
        df_sales_agg_with_totals.select("order_id", "revenue_check").show()

        # If aggregation exists, write it to Gold layer
        if df_sales_agg.count() > 0:
            df_sales_agg_with_totals.write.mode("overwrite").csv(str(gold_layer_path / "fact_sales.csv"), header=True)

            # Update processed_gold and save to config.json
            processed_gold.add(today_date)
            config["processed_gold"] = list(processed_gold)
            with open(config_path, "w") as config_file:
                json.dump(config, config_file, indent=4)

            print(f"Gold layer written: {gold_layer_path}")

print("Gold layer processing complete.")
