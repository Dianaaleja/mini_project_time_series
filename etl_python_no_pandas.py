import psycopg2

# --- Extract: connect to source database and extract data ---
def extract_data():
    conn = psycopg2.connect(
        dbname="source_db",
        user="user",
        password="password",
        host="localhost"
    )
    cur = conn.cursor()
    cur.execute("SELECT id, customer_name, total_amount FROM orders")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# --- Transform: filter or clean data using Python ---
def transform_data(rows):
    # Keep only orders over 100 EUR
    return [row for row in rows if row[2] > 100]

# --- Load: insert data into target database ---
def load_data(transformed_rows):
    conn = psycopg2.connect(
        dbname="target_db",
        user="user",
        password="password",
        host="localhost"
    )
    cur = conn.cursor()
    insert_query = "INSERT INTO clean_orders (id, customer_name, total_amount) VALUES (%s, %s, %s)"
    cur.executemany(insert_query, transformed_rows)
    conn.commit()
    cur.close()
    conn.close()

# --- Run ETL process ---
if __name__ == "__main__":
    raw_data = extract_data()
    clean_data = transform_data(raw_data)
    load_data(clean_data)
    print("ETL process completed.")
