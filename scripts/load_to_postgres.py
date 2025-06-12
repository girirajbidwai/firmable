import pandas as pd
import psycopg2
from psycopg2 import sql

# === Configuration ===
DB_CONFIG = {
    "host": "your_host",           # e.g., 'localhost' or 'aws-0-ap-south-1.pooler.supabase.com'
    "port": 5432,
    "dbname": "your_db_name",
    "user": "your_username",
    "password": "your_password"
}

COMMON_CRAWL_CSV = "company_industry_output.csv"  # ✅ Replace with your file
ABN_CSV = "abn_data.csv"  # ✅ Replace with your file

# === Table Names ===
COMMON_CRAWL_TABLE = "commoncrawl_companies"
ABN_TABLE = "abn_entities"

# === Create Table Queries ===
CREATE_COMMON_CRAWL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {COMMON_CRAWL_TABLE} (
    url TEXT,
    company_name TEXT,
    industry TEXT
);
"""

CREATE_ABN_TABLE = f"""
CREATE TABLE IF NOT EXISTS {ABN_TABLE} (
    abn TEXT,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    entity_address TEXT,
    entity_postcode TEXT,
    entity_state TEXT,
    entity_start_date DATE
);
"""

# === Load CSV to PostgreSQL ===
def load_csv_to_postgres(csv_path, table_name, create_query, conn):
    cursor = conn.cursor()
    cursor.execute(create_query)
    conn.commit()

    # Load data using pandas
    df = pd.read_csv(csv_path)

    # Normalize column names to lowercase
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Write data in chunks
    for i in range(0, len(df), 1000):
        chunk = df.iloc[i:i+1000]
        cols = ','.join(chunk.columns)
        values = ','.join(['%s'] * len(chunk.columns))
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, chunk.columns)),
            sql.SQL(values)
        )
        for row in chunk.itertuples(index=False, name=None):
            cursor.execute(insert_query, row)
        conn.commit()
        print(f"Inserted rows {i} to {i + len(chunk)} into {table_name}")

    cursor.close()
    print(f"✅ Finished loading {csv_path} into {table_name}")

# === Main Script ===
def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("📡 Connected to PostgreSQL")

        # Load Common Crawl data
        load_csv_to_postgres(COMMON_CRAWL_CSV, COMMON_CRAWL_TABLE, CREATE_COMMON_CRAWL_TABLE, conn)

        # Load ABN data
        load_csv_to_postgres(ABN_CSV, ABN_TABLE, CREATE_ABN_TABLE, conn)

        conn.close()
        print("🔌 Connection closed.")
    except Exception as e:
        print("❌ Error:", e)

if __name__ == "__main__":
    main()
