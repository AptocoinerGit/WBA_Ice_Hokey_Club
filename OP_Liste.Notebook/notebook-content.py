# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b0e44a15-7f22-49ea-ac65-42326e33dd45",
# META       "default_lakehouse_name": "Lakehouse_1",
# META       "default_lakehouse_workspace_id": "4d07b2b6-e88f-49a3-bfb1-90436e1d2cf7",
# META       "known_lakehouses": [
# META         {
# META           "id": "b0e44a15-7f22-49ea-ac65-42326e33dd45"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import pandas as pd
from io import StringIO

# Your download URL
url = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html"

# Username & password
username = "test"
password = "test"

# Fetch the data using Basic Auth
response = requests.get(url, auth=(username, password))

# Check response status
if response.status_code == 200:
    print(" Successfully downloaded the CSV file")

    # Convert HTML or CSV text to DataFrame
    try:
        df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
    except Exception as e:
        print(" Could not read as CSV directly, trying as HTML table...")
        df = pd.read_html(response.text, header=0, decimal=',', thousands='.')[0]
    
    display(df.head())

    # 🧹 Clean column names (remove spaces and invalid chars)
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )

    # 👉 Save to Lakehouse table
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable("OP_Liste")

else:
    print(f" Download failed: {response.status_code}")
    print(response.text[:500])  # Print a snippet for debugging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
