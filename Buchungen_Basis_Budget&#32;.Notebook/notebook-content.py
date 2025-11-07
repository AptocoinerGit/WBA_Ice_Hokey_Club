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
url = "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html"

# Username & password
username = "test"
password = "test"

# Fetch the data using Basic Auth
response = requests.get(url, auth=(username, password))

# Check response status
if response.status_code == 200:
    print("✅ Successfully downloaded the file")

    # Try reading as CSV first
    try:
        df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
    except Exception:
        print("⚙️ Could not read as CSV directly, trying as HTML table...")
        df = pd.read_html(response.text, header=0)[0]

    # 🧹 Clean column names
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )

    # --- 🧾 Fix European number formats ---
    numeric_cols = ["Soll", "Haben", "Betrag"]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)  # Remove thousand separator
                .str.replace(",", ".", regex=False)  # Replace comma with dot
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    display(df.head())

    # 👉 Save to Lakehouse table
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable("Buchungen_Basis_Budget")

else:
    print(f"❌ Download failed: {response.status_code}")
    print(response.text[:500])  # Print a snippet for debugging


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
