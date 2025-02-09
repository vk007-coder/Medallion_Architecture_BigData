# 🏆 Medallion Architecture Data Pipeline (Bronze → Silver → Gold)

## 🚀 Overview
This project implements a **Medallion Architecture pipeline** using PySpark to process structured data through **Bronze, Silver, and Gold layers**. The pipeline ensures **data validation, transformation, and aggregation** while maintaining data integrity and business rules.

---

## 📌 Pipeline Flow

| 🔷 Layer | 📄 Purpose | 🔄 Operations |
|---------|----------|-------------|
| 🟤 **Bronze** | Raw Data Ingestion | Ingests raw CSV files from multiple sources into the Bronze layer. |
| ⚪ **Silver** | Data Cleansing & Validation | Deduplication, schema enforcement, data validation, and relationship checks. |
| 🟡 **Gold** | Aggregated Insights | Aggregates revenue, order stats, and generates reports for analytics. |

---

## 📂 Folder Structure

```
📦 Medallion_Pipeline
 ┣ 📂 Bronze
 ┃ ┣ 📂 raw_Orders
 ┃ ┣ 📂 raw_Customers
 ┃ ┣ 📂 raw_Order_Items
 ┃ ┣ 📂 raw_Products
 ┃ ┗ 📜 bronze_processing.py
 ┣ 📂 Silver
 ┃ ┣ 📂 orders_valid
 ┃ ┣ 📂 customers_valid
 ┃ ┣ 📂 order_items_valid
 ┃ ┣ 📂 products_valid
 ┃ ┗ 📜 silver_processing.py
 ┣ 📂 Gold
 ┃ ┣ 📜 fact_sales.csv
 ┃ ┗ 📜 gold_processing.py
 ┣ 📜 config.json
 ┗ 📜 README.md
```

---

## 🔧 Layer Processing Details

### 🟤 Bronze Layer Processing (Raw Data Ingestion)
📜 **Script**: `bronze_processing.py`
#### 🔹 Key Features:
- Reads raw CSV files from multiple directories.
- Stores data in the Bronze layer without transformations.
- Ensures data availability for further processing.

---

### ⚪ Silver Layer Processing (Data Validation & Cleaning)
📜 **Script**: `silver_processing.py`
#### 🔹 Key Features:
- Deduplicates data.
- Ensures mandatory fields are not NULL.
- Joins tables to validate relationships (e.g., orders ↔ customers).
- Filters incorrect records (e.g., negative prices, missing IDs).
- Saves cleaned & validated data to the Silver layer.

#### 🏗 **Silver Schema Example:**

| Column Name | Data Type | Description |
|------------|----------|-------------|
| `order_id` | STRING | Unique order identifier |
| `customer_id` | STRING | Unique customer identifier |
| `product_id` | STRING | Unique product identifier |
| `unit_price` | DECIMAL | Price per unit of product |
| `quantity` | INT | Number of products ordered |

---

### 🟡 Gold Layer Processing (Aggregated Insights)
📜 **Script**: `gold_processing.py`
#### 🔹 Key Features:
- Joins orders, customers, products, and order items.
- Filters out canceled orders.
- Aggregates revenue per order.
- Checks for data integrity issues (e.g., revenue mismatches).
- Saves transformed data to the Gold layer as `fact_sales.csv`.

#### 🏗 **Gold Output Example:**

| `order_id` | `customer_id` | `total_revenue` | `total_items` |
|------------|--------------|----------------|--------------|
| ORD001 | CUST123 | 500.00 | 5 |
| ORD002 | CUST456 | 1200.00 | 10 |

---

## 🗂 Configuration File (`config.json`)

Stores **input/output paths**, **processing flags**, and **metadata**.

```json
{
  "Bronze_paths": {
    "B_output_folder": "./Bronze/",
    "sub_dirs": ["raw_Orders", "raw_Customers", "raw_Order_Items", "raw_Products"]
  },
  "Silver_path": {
    "s_input_Path": "./Silver/",
    "s_Output_Path": "./Silver/"
  },
  "Gold_Paths": {
    "g_input_Path": "./Silver/",
    "g_output_Path": "./Gold/"
  },
  "processed_silver": [],
  "processed_gold": []
}
```

---

## 🏃‍♂️ Execution Steps

Run the following scripts **sequentially**:

```bash
# Step 1: Process Bronze Layer
python bronze_processing.py

# Step 2: Process Silver Layer
python silver_processing.py

# Step 3: Process Gold Layer
python gold_processing.py
```

📝 Shell Script for Automation
To automate the execution of all steps, use the provided shell script run_pipeline.sh.

📂 run_pipeline.sh
bash
Copy
Edit
#!/bin/bash

# Set environment variables for paths (optional)
export BRONZE_PATH="./Bronze/"
export SILVER_PATH="./Silver/"
export GOLD_PATH="./Gold/"

# Function to run a step in the pipeline
run_step() {
  script_name=$1
  echo "Running $script_name..."
  python $script_name
  if [ $? -ne 0 ]; then
    echo "Error: $script_name failed!"
    exit 1
  else
    echo "$script_name completed successfully."
  fi
}

*# Step 1: Process Bronze Layer*
run_step "bronze_processing.py"

*# Step 2: Process Silver Layer*
run_step "silver_processing.py"

*# Step 3: Process Gold Layer*
run_step "gold_processing.py"
**
#echo "Pipeline execution completed successfully!"#
🏃‍♂️ Running the Shell Script
Make the script executable:

bash
Copy
Edit
chmod +x run_pipeline.sh
Run the script:

bash
Copy
Edit
./run_pipeline.sh**

---

## 🚀 Future Enhancements

✔️ Automate pipeline execution using **Apache Airflow**  
✔️ Store data in **Parquet format** for better performance  
✔️ Implement **real-time data ingestion with Kafka**  
✔️ Add **error handling & alerting mechanisms**  
✔️ Extend pipeline for **machine learning & analytics**  

---

## 🎯 Conclusion
This **Medallion Architecture Data Pipeline** ensures structured, validated, and insightful data processing for analytics and business intelligence. 🚀

