#!/bin/bash

# Set the date variable if an argument is passed
if [ -z "$1" ]; then
    DATE=$(date +%Y%m%d)  # Default to today's date
else
    DATE=$1  # Use the passed date argument
fi

# Log the date being used
echo "[INFO] Using date: $DATE"

# Check for Python installation
echo "[INFO] Checking for Python installation..."
python --version &> /dev/null
if [ $? -eq 0 ]; then
    PYTHON_VERSION=$(python --version 2>&1)
    echo "[INFO] Found Python: $PYTHON_VERSION"
else
    echo "[ERROR] Python is not installed. Exiting!"
    exit 1
fi

# Check for config file in the current directory
if [ -f "config.json" ]; then
    echo "[INFO] Config file found."
else
    echo "[ERROR] config.json not found. Exiting!"
    exit 1
fi

# Run Bronze Layer
echo "[INFO] Running Bronze Layer..."
python Bronze_layer.py $DATE
if [ $? -eq 0 ]; then
    echo "[INFO] Bronze layer processing complete!"
else
    echo "[ERROR] Bronze Layer failed. Exiting!"
    exit 1
fi

# Run Silver Layer
echo "[INFO] Running Silver Layer..."
python Silver_layer.py $DATE
if [ $? -eq 0 ]; then
    echo "[INFO] Silver layer processing complete!"
else
    # Check if Silver Layer was skipped based on logs
    if grep -q "Silver data for $DATE has already been processed. Skipping..." "Silver_layer_output.log"; then
        echo "[INFO] Silver Layer skipped, no changes made."
    else
        echo "[ERROR] Silver Layer failed. Exiting!"
        exit 1
    fi
fi

# Run Gold Layer
echo "[INFO] Running Gold Layer..."
python Gold_layer.py $DATE
if [ $? -eq 0 ]; then
    echo "[INFO] Gold layer processing complete!"
else
    echo "[ERROR] Gold Layer failed. Exiting!"
    exit 1
fi

echo "[INFO] Pipeline executed successfully!"
exit 0
