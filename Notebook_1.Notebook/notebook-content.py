# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "194920f9-797f-4ad3-bb97-37f0c6ea9bf5",
# META       "default_lakehouse_name": "BWA_LakeHouse",
# META       "default_lakehouse_workspace_id": "8e5d0bf9-3ed0-4c17-b0e2-cb0cdd1f534d",
# META       "known_lakehouses": [
# META         {
# META           "id": "194920f9-797f-4ad3-bb97-37f0c6ea9bf5"
# META         },
# META         {
# META           "id": "b0e44a15-7f22-49ea-ac65-42326e33dd45"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import requests
from io import BytesIO
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# --- 🔧 Configuration: Table names + Dropbox URLs ---
tables = [
    {
        "name": "buchungen_basisdropbox",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html"
    },
    {
        "name": "op_list11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html"
    },
    {
        "name": "kreditoren_gesamtumsatz_11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mbugbsdknjymywtiyjvgewtimbwguwtqojxmiwtcmbtmu3wmzlemqzdmzq/Kreditoren_Gesamtumsatz_.html"
    },
    {
        "name": "kontaktedropbox",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html"
    },
    {
        "name": "kreditoren11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html"
    },
    {
        "name": "Buchungen_Basis_Budget",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html"
    },
    {
        "name": "BWA_Gliederung",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6m3cgiygczlbg4wtgztdhewtimzwmiwtsytfhewtmojvguytonjymyytkoa/BWA_Gliederung.html"
    },
    {
        "name": "SCRBWA_Gliederung",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6zbygfrdgmrqmiwtcmrtmuwtizbthewwcmrqgqwwcndbmiydmytfgy4doma/SCRBWA_Gliederung.html"
    }
]

# --- 🔐 Authentication credentials ---
username = "test"
password = "test"

# --- 🧹 Common cleaning function ---
def clean_dataframe(df):
    """Standard cleaning logic for all Excel files."""
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )
    df = df.replace([float("inf"), float("-inf")], pd.NA).fillna("")

    numeric_cols = ["Soll", "Haben", "Betrag"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in df.columns:
        if col not in numeric_cols:
            df[col] = df[col].astype(str)

    return df


# --- 🔁 Loop through all configured tables ---
for t in tables:
    print(f"\n===================================================")
    print(f"🚀 Processing table: {t['name']}")
    print(f"===================================================")

    try:
        # Step 1️⃣ — Fetch file with Basic Authentication
        response = requests.get(t["url"], auth=(username, password))
        response.raise_for_status()

        # Step 2️⃣ — Try to read using UTF-8, fallback to latin1 if needed
        try:
            df = pd.read_html(BytesIO(response.content), encoding="utf-8")[0]
        except UnicodeDecodeError:
            print("⚠️ UTF-8 failed, retrying with latin1 encoding...")
            df = pd.read_html(BytesIO(response.content), encoding="latin1")[0]

        print(f"✅ Loaded {len(df)} records from server")

        # Step 3️⃣ — Clean columns and data
        df = clean_dataframe(df)

        # Step 4️⃣ — Convert to Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Step 5️⃣ — Identify key column automatically
        key_col = None
        for possible_key in ["MasterID", "Kontakt_MasterID", "master_id"]:
            if possible_key in df.columns:
                key_col = possible_key
                break

        # Step 6️⃣ — Load into Lakehouse
        if spark.catalog.tableExists(t["name"]):
            print(f"🔍 Table {t['name']} exists — checking for new records...")

            if key_col:
                existing_keys = spark.table(t["name"]).select(key_col)
                new_records = sdf.join(existing_keys, on=key_col, how="left_anti")
                new_count = new_records.count()

                if new_count > 0:
                    print(f"🆕 Found {new_count} new records — inserting...")
                    new_records.write.mode("append").saveAsTable(t["name"])
                else:
                    print("✅ No new records found.")
            else:
                print(f"⚠️ No key column found — overwriting table.")
                sdf.write.mode("overwrite").saveAsTable(t["name"])
        else:
            print(f"⚙️ Table does not exist — creating new table...")
            sdf.write.mode("overwrite").saveAsTable(t["name"])

        print(f"✅ Load completed for {t['name']} ({sdf.count()} records)")

    except Exception as e:
        print(f"❌ Error processing {t['name']}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
from io import StringIO, BytesIO
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# --- 🔧 Table configuration (URLs + table names) ---
tables = [
    {
        "name": "buchungen_basisdropbox",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html"
    },
    {
        "name": "op_list11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html"
    },
    {
        "name": "kreditoren_gesamtumsatz_11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mbugbsdknjymywtiyjvgewtimbwguwtqojxmiwtcmbtmu3wmzlemqzdmzq/Kreditoren_Gesamtumsatz_.html"
    },
    {
        "name": "kontaktedropbox",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html"
    },
    {
        "name": "kreditoren11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html"
    },
{
        "name": "Buchungen_Basis_Budget11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html"
    },
{
        "name": "BWA_Gliederung11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6m3cgiygczlbg4wtgztdhewtimzwmiwtsytfhewtmojvguytonjymyytkoa/BWA_Gliederung.html"
    },
{
        "name": "SCRBWA_Gliederung11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6zbygfrdgmrqmiwtcmrtmuwtizbthewwcmrqgqwwcndbmiydmytfgy4doma/SCRBWA_Gliederung.html"
    }

]

# --- 🔐 Credentials ---
username = "test"
password = "test"

# --- 🧹 Cleaning Function ---
def clean_dataframe(df):
    """Standard cleaning logic for all loaded tables."""
    # Clean column names
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )

    # Replace infinities and NaN with blanks
    df = df.replace([float("inf"), float("-inf")], pd.NA).fillna("")

    # Convert numeric columns if they exist
    numeric_cols = ["Soll", "Haben", "Betrag"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# --- 🔁 Process each table ---
for t in tables:
    print(f"\n{'='*60}")
    print(f"🚀 Processing table: {t['name']}")
    print(f"{'='*60}")

    try:
        # Step 1️⃣ — Fetch file using Basic Auth
        response = requests.get(t["url"], auth=(username, password))
        response.raise_for_status()

        # Step 2️⃣ — Try to read as CSV, else fallback to HTML
        try:
            df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
            print("✅ Loaded as CSV format")
        except Exception:
            print("⚠️ Could not read as CSV — trying as HTML...")
            try:
                df = pd.read_html(BytesIO(response.content), header=0, encoding="utf-8")[0]
                print("✅ Loaded as HTML table")
            except UnicodeDecodeError:
                df = pd.read_html(BytesIO(response.content), header=0, encoding="latin1")[0]
                print("✅ Loaded as HTML (latin1 fallback)")

        print(f"📊 Rows fetched: {len(df)}")

        # Step 3️⃣ — Clean data
        df = clean_dataframe(df)

        # Step 4️⃣ — Convert to Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Step 5️⃣ — Detect key column
        key_col = None
        for possible_key in ["MasterID", "Kontakt_MasterID", "master_id"]:
            if possible_key in df.columns:
                key_col = possible_key
                break

        # Step 6️⃣ — Append or overwrite logic
        if spark.catalog.tableExists(t["name"]):
            print(f"🔍 Table {t['name']} already exists")
            if key_col:
                existing_keys = spark.table(t["name"]).select(key_col)
                new_records = sdf.join(existing_keys, on=key_col, how="left_anti")
                new_count = new_records.count()

                if new_count > 0:
                    print(f"🆕 Found {new_count} new records — appending...")
                    new_records.write.mode("append").saveAsTable(t["name"])
                else:
                    print("✅ No new records to insert. Table up to date.")
            else:
                print("⚠️ No key column found — overwriting table.")
                sdf.write.mode("overwrite").saveAsTable(t["name"])
        else:
            print(f"⚙️ Table not found — creating fresh table.")
            sdf.write.mode("overwrite").saveAsTable(t["name"])

        print(f"✅ Load completed for {t['name']} ({sdf.count()} records)")

    except Exception as e:
        print(f"❌ Error processing {t['name']}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
from io import StringIO, BytesIO
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# --- 🔧 Table configuration ---
tables = [
    {
        "name": "buchungen_basisdropbox",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html"
    },
    {
        "name": "op_list11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html"
    },
    {
        "name": "kreditoren_gesamtumsatz_11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mbugbsdknjymywtiyjvgewtimbwguwtqojxmiwtcmbtmu3wmzlemqzdmzq/Kreditoren_Gesamtumsatz_.html"
    },
    {
        "name": "kontaktedropbox",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html"
    },
    {
        "name": "kreditoren11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html"
    },
{
        "name": "Buchungen_Basis_Budget11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html"
    },
{
        "name": "BWA_Gliederung11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6m3cgiygczlbg4wtgztdhewtimzwmiwtsytfhewtmojvguytonjymyytkoa/BWA_Gliederung.html"
    },
{
        "name": "SCRBWA_Gliederung11",
        "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6zbygfrdgmrqmiwtcmrtmuwtizbthewwcmrqgqwwcndbmiydmytfgy4doma/SCRBWA_Gliederung.html"
    }

]

# --- 🔐 Credentials ---
username = "test"
password = "test"

# --- 🧹 Cleaning Function ---
def clean_dataframe(df):
    """Standard cleaning logic for all loaded tables."""
    # --- Step 1️⃣: Handle duplicate column names ---
    def make_unique(cols):
        seen = {}
        new_cols = []
        for c in cols:
            if c in seen:
                seen[c] += 1
                new_cols.append(f"{c}_{seen[c]}")
            else:
                seen[c] = 0
                new_cols.append(c)
        return new_cols

    df.columns = make_unique(df.columns)

    # --- Step 2️⃣: Clean column names ---
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )

    # --- Step 3️⃣: Replace infinities & NaN with blanks ---
    df = df.replace([float("inf"), float("-inf")], pd.NA).fillna("")

    # --- Step 4️⃣: Convert numeric columns (if exist) ---
    numeric_cols = ["Soll", "Haben", "Betrag"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # --- Step 5️⃣: Convert everything else to string ---
    df = df.astype(str)

    return df


# --- 🔁 Process each table ---
for t in tables:
    print(f"\n{'='*60}")
    print(f"🚀 Processing table: {t['name']}")
    print(f"{'='*60}")

    try:
        # Step 1️⃣ — Fetch file
        response = requests.get(t["url"], auth=(username, password))
        response.raise_for_status()

        # Step 2️⃣ — Try CSV, fallback to HTML
        try:
            df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
            print("✅ Loaded as CSV format")
        except Exception:
            print("⚠️ Could not read as CSV — trying as HTML...")
            try:
                df = pd.read_html(BytesIO(response.content), header=0, encoding="utf-8")[0]
                print("✅ Loaded as HTML table")
            except UnicodeDecodeError:
                df = pd.read_html(BytesIO(response.content), header=0, encoding="latin1")[0]
                print("✅ Loaded as HTML (latin1 fallback)")

        print(f"📊 Rows fetched: {len(df)}")

        # Step 3️⃣ — Clean data
        df = clean_dataframe(df)

        # Step 4️⃣ — Convert to Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Step 5️⃣ — Detect key column automatically
        key_col = None
        for possible_key in ["MasterID", "Kontakt_MasterID", "master_id"]:
            if possible_key in df.columns:
                key_col = possible_key
                break

        # Step 6️⃣ — Incremental or full load
        if spark.catalog.tableExists(t["name"]):
            print(f"🔍 Table {t['name']} already exists")
            if key_col:
                existing_keys = spark.table(t["name"]).select(key_col)
                new_records = sdf.join(existing_keys, on=key_col, how="left_anti")
                new_count = new_records.count()

                if new_count > 0:
                    print(f"🆕 Found {new_count} new records — appending...")
                    new_records.write.mode("append").saveAsTable(t["name"])
                else:
                    print("✅ No new records found. Table up to date.")
            else:
                print("⚠️ No key column found — overwriting table.")
                sdf.write.mode("overwrite").saveAsTable(t["name"])
        else:
            print(f"⚙️ Table not found — creating new table...")
            sdf.write.mode("overwrite").saveAsTable(t["name"])

        print(f"✅ Load completed for {t['name']} ({sdf.count()} records)")

    except Exception as e:
        print(f"❌ Error processing {t['name']}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
from io import StringIO, BytesIO
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# --- 🔧 Table configuration ---
tables = [
    {"name": "buchungen_basisdropbox",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obumvsggnrqmuwtimrwmuwtizdegqwweodgmywtaztcgzswenjrmqydomy/Buchungen_Basis.html"},
    {"name": "op_list11",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6y3emeytombxgqwwiyleguwtizbwmywwenrxmuwtczjrmy3tenbxguygimi/OP_Liste.html"},
    {"name": "kreditoren_gesamtumsatz_11",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mbugbsdknjymywtiyjvgewtimbwguwtqojxmiwtcmbtmu3wmzlemqzdmzq/Kreditoren_Gesamtumsatz_.html"},
    {"name": "kontaktedropbox",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6obvgfrgcnlggywweodfhawtiyzvgewtqmzwgqwtinrqmnrtsmlbgazwkma/Kontakte.html"},
    {"name": "kreditoren11",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6nztgqydomrsg4wtsntgmywtinzwgawtsnbygqwtcmrqgfsdkmzwgntgkzq/Kreditoren.html"},
    {"name": "Buchungen_Basis_Budget11",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6mrtgvsdsn3bmiwtmndgguwtizjrg4wwcmlegywwczbrgzrwknjvhbtdcyy/Buchungen_Basis_Budget.html"},
    {"name": "BWA_Gliederung11",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6m3cgiygczlbg4wtgztdhewtimzwmiwtsytfhewtmojvguytonjymyytkoa/BWA_Gliederung.html"},
    {"name": "SCRBWA_Gliederung11",
     "url": "https://appload.scopevisio.com/datasource/giydinbuhe4s6mjxf52xgzlsl42demjtmjqtcmjnmmzdcmzngqydonjnhfstkzbngjtdozjrgbsggmlggnsc6zbygfrdgmrqmiwtcmrtmuwtizbthewwcmrqgqwwcndbmiydmytfgy4doma/SCRBWA_Gliederung.html"}
]

# ---  Credentials ---
username = "test"
password = "test"

# ---  Cleaning Function ---
def clean_dataframe(df):
    """Clean column names and values for Spark safety."""
    
    # Step 1️: Clean & make column names unique (Spark-safe)
    def clean_column_names(cols):
        seen = {}
        new_cols = []
        for c in cols:
            new_c = (
                str(c)
                .strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )
            new_c = ''.join(ch for ch in new_c if ch.isalnum() or ch == "_")
            if new_c in seen:
                seen[new_c] += 1
                new_cols.append(f"{new_c}_{seen[new_c]}")
            else:
                seen[new_c] = 0
                new_cols.append(new_c)
        return new_cols

    df.columns = clean_column_names(df.columns)

    # Step 2️: Replace infinities & NaNs
    df = df.replace([float("inf"), float("-inf")], pd.NA).fillna("")

    # Step 3️: Convert numeric columns (common accounting fields)
    for col in ["soll", "haben", "betrag"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(".", " ", regex=False)  #this transformation is for indian calc. but not german remove it in deployment
                .str.replace(",", ".", regex=False)  #this transformation is for indian calc. but not german remove it in deployment
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Step 4️: Convert all columns to string
    df = df.astype(str)

    return df


# ---  Process each table ---
for t in tables:
    print(f"\n{'='*60}")
    print(f" Processing table: {t['name']}")
    print(f"{'='*60}")

    try:
        # Step 1️ — Fetch file
        response = requests.get(t["url"], auth=(username, password))
        response.raise_for_status()
        print(response.raise_for_status())

        # Step 2️ — Try CSV, fallback to HTML
        try:
            df = pd.read_csv(StringIO(response.text), sep=';', encoding='latin1')
            print(" Loaded as CSV format")
        except Exception:
            print(" Could not read as CSV — trying as HTML...")
            try:
                df = pd.read_html(BytesIO(response.content), header=0, encoding="utf-8")[0]
                print(" Loaded as HTML table")
            except UnicodeDecodeError:
                df = pd.read_html(BytesIO(response.content), header=0, encoding="latin1")[0]
                print("Loaded as HTML (latin1 fallback)")

        print(f" Rows fetched: {len(df)}")

        # Step 3️ — Clean data
        df = clean_dataframe(df)

        # Step 4️ — Convert to Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Step 5️ — Find key column dynamically
        key_col = None
        for possible_key in ["masterid", "kontakt_masterid", "master_id","kontakt_master_id"]:
            if possible_key in df.columns:
                key_col = possible_key
                break
 
        # Step 6️ — Load logic
        if spark.catalog.tableExists(t["name"]):
            print(f"Table {t['name']} already exists")
            if key_col:
                existing_keys = spark.table(t["name"]).select(key_col)
                new_records = sdf.join(existing_keys, on=key_col, how="left_anti")
                new_count = new_records.count()

                if new_count > 0:
                    print(f"Found {new_count} new records — appending...")
                    new_records.write.mode("append").saveAsTable(t["name"])
                else:
                    print(" No new records found. Table up to date.")
            else:
                print(" No key column found — overwriting table.")
                sdf.write.mode("overwrite").saveAsTable(t["name"])
        else:
            print(f" Table not found — creating new table...")
            sdf.write.mode("overwrite").saveAsTable(t["name"])

        print(f" Load completed for {t['name']} ({sdf.count()} records)")

    except Exception as e:
        print(f" Error processing {t['name']}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lst = [1,2,3]
i = 1
if i == i :
    print (i)

else: 
    print('no i present')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# for testing


# CELL ********************

import pandas as pd
import requests
from io import BytesIO
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# --- Configuration for all tables ---
tables = [
    {"name": "buchungen_basisdropbox",
     "url": "https://www.dropbox.com/scl/fi/xm1euz8701nigfzokar5v/BWA.xlsx?rlkey=mxdk1tn86dah7c44valrc0a53&st=6bdj2xv9&dl=1"},
    {"name": "op_list11",
     "url": "https://www.dropbox.com/scl/fi/vt9i43mw99gybvw2dxj48/op_liste_11.xlsx?rlkey=yd1ev8dpqb4dlhujk3s7wkteh&st=vgcc03u4&dl=1"},
    {"name": "kreditoren_gesamtumsatz_11",
     "url": "https://www.dropbox.com/scl/fi/gga6yx6cz9n9xnx69gpbu/kreditoren_gesamtumsatz_11.xlsx?rlkey=fgb792vkvvtkamkfqvymfaz7j&st=wnrhw5ww&dl=1"},
    {"name": "kontaktedropbox",
     "url": "https://www.dropbox.com/scl/fi/ghcrj0m4m9b0udrktg02p/kontaktedropbox.xlsx?rlkey=cfzmq1lxlw63iuw4gzd7tj4u7&st=scpzih9h&dl=1"},
    {"name": "kreditoren11",
     "url": "https://www.dropbox.com/scl/fi/zekzentz6brg0m0w6funz/kreditoren11.xlsx?rlkey=ewlf1epcpamngx6kndt95mxf7&st=hu8rvmsc&dl=1"}
]


# --- Function to clean column names ---
def clean_column_names(cols):
    seen = {}
    new_cols = []
    for c in cols:
        new_c = (
            str(c)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        new_c = ''.join(ch for ch in new_c if ch.isalnum() or ch == "_")
        if new_c in seen:
            seen[new_c] += 1
            new_cols.append(f"{new_c}_{seen[new_c]}")
        else:
            seen[new_c] = 0
            new_cols.append(new_c)
    return new_cols


# --- Function to clean full DataFrame ---
def clean_dataframe(df):
    df.columns = clean_column_names(df.columns)
    df = df.fillna("")

    df = df.astype(str)
    return df


# --- 🔁 Loop through all configured tables ---
for t in tables:
    print(f"\n===================================================")
    print(f"🚀 Processing table: {t['name']}")
    print(f"===================================================")

    try:
        # Step 1️⃣ — Load Excel file from Dropbox
        response = requests.get(t["url"])
        response.raise_for_status()
        df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
        print(f"✅ Loaded {len(df)} records from Dropbox")

        # Step 2️⃣ — Clean columns and data
        df = clean_dataframe(df)

        # Step 3️⃣ — Convert to Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Step 4️⃣ — Identify key column automatically
        key_col = None
        for possible_key in ["masterid", "kontakt_masterid", "master_id","kontakt_master_id"]:
            if possible_key in df.columns:
                key_col = possible_key
                break

        # Step 5️⃣ — Load into Lakehouse (incremental/full)
        if spark.catalog.tableExists(t["name"]):
            print(f"🔍 Table {t['name']} exists — checking for new records...")
            if key_col:
                existing_keys = spark.table(t["name"]).select(key_col)
                new_records = sdf.join(existing_keys, on=key_col, how="left_anti")
                new_count = new_records.count()

                if new_count > 0:
                    print(f"🆕 Found {new_count} new records — inserting...")
                    new_records.write.mode("append").saveAsTable(t["name"])
                else:
                    print("✅ No new records found.")
            else:
                print(f"⚠️ No key column found — overwriting table.")
                sdf.write.mode("overwrite").saveAsTable(t["name"])
        else:
            print(f"⚙️ Table does not exist — creating new table...")
            sdf.write.mode("overwrite").saveAsTable(t["name"])

        print(f"✅ Load completed for {t['name']} ({sdf.count()} records)")

    except Exception as e:
        print(f"❌ Error processing {t['name']}: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
from io import BytesIO
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.getOrCreate()

# --- Configuration for all tables ---
tables = [
    {"name": "buchungen_basisdropbox",
     "url": "https://www.dropbox.com/scl/fi/xm1euz8701nigfzokar5v/BWA.xlsx?rlkey=mxdk1tn86dah7c44valrc0a53&st=6bdj2xv9&dl=1"},
    {"name": "op_list11",
     "url": "https://www.dropbox.com/scl/fi/vt9i43mw99gybvw2dxj48/op_liste_11.xlsx?rlkey=yd1ev8dpqb4dlhujk3s7wkteh&st=vgcc03u4&dl=1"},
    {"name": "kreditoren_gesamtumsatz_11",
     "url": "https://www.dropbox.com/scl/fi/gga6yx6cz9n9xnx69gpbu/kreditoren_gesamtumsatz_11.xlsx?rlkey=fgb792vkvvtkamkfqvymfaz7j&st=wnrhw5ww&dl=1"},
    {"name": "kontaktedropbox",
     "url": "https://www.dropbox.com/scl/fi/ghcrj0m4m9b0udrktg02p/kontaktedropbox.xlsx?rlkey=cfzmq1lxlw63iuw4gzd7tj4u7&st=scpzih9h&dl=1"},
    {"name": "kreditoren11",
     "url": "https://www.dropbox.com/scl/fi/zekzentz6brg0m0w6funz/kreditoren11.xlsx?rlkey=ewlf1epcpamngx6kndt95mxf7&st=hu8rvmsc&dl=1"}
]


# --- Function to clean column names ---
def clean_column_names(cols):
    seen = {}
    new_cols = []
    for c in cols:
        new_c = (
            str(c)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )
        new_c = ''.join(ch for ch in new_c if ch.isalnum() or ch == "_")
        if new_c in seen:
            seen[new_c] += 1
            new_cols.append(f"{new_c}_{seen[new_c]}")
        else:
            seen[new_c] = 0
            new_cols.append(new_c)
    return new_cols


# --- Function to clean full DataFrame ---
def clean_dataframe(df):
    df.columns = clean_column_names(df.columns)
    df = df.fillna("")
    df = df.astype(str)
    return df


# --- 🔁 Loop through all configured tables (one by one) ---
for t in tables:
    print("\n" + "=" * 55)
    print(f"🚀 Processing table: {t['name']}")
    print("=" * 55)

    try:
        # Step 1️⃣ — Load Excel file from Dropbox
        response = requests.get(t["url"])
        response.raise_for_status()
        df = pd.read_excel(BytesIO(response.content), engine="openpyxl")
        print(f"✅ Loaded {len(df)} records from Dropbox")

        # Step 2️⃣ — Clean columns and data
        df = clean_dataframe(df)

        # Step 3️⃣ — Convert to Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Step 4️⃣ — Identify key column automatically
        key_col = None
        for possible_key in ["masterid", "kontakt_masterid", "master_id", "kontakt_master_id"]:
            if possible_key in df.columns:
                key_col = possible_key
                break

        # Step 5️⃣ — Load into Lakehouse (incremental/full)
        if spark.catalog.tableExists(t["name"]):
            print(f"🔍 Table {t['name']} exists — checking for new records...")
            if key_col:
                existing_keys = spark.table(t["name"]).select(key_col)
                new_records = sdf.join(existing_keys, on=key_col, how="left_anti")
                new_count = new_records.count()

                if new_count > 0:
                    print(f"🆕 Found {new_count} new records — inserting...")
                    new_records.write.mode("append").saveAsTable(t["name"])
                else:
                    print("✅ No new records found.")
            else:
                print(f"⚠️ No key column found — overwriting table.")
                sdf.write.mode("overwrite").saveAsTable(t["name"])
        else:
            print(f"⚙️ Table does not exist — creating new table...")
            sdf.write.mode("overwrite").saveAsTable(t["name"])

        # Step 6️⃣ — Confirm completion
        print(f"✅ Load completed for {t['name']} ({sdf.count()} records)")
        
        # ✅ Force Spark to finish this table completely before next starts
        spark.catalog.clearCache()
        time.sleep(1)  # small pause to separate logs cleanly

    except Exception as e:
        print(f"❌ Error processing {t['name']}: {e}")
        spark.catalog.clearCache()
        time.sleep(1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
