import psycopg2, csv
from pathlib import Path
from datetime import datetime

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS transactions")
    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("DROP TABLE IF EXISTS accounts")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
        customer_id VARCHAR(255) PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        address_1 VARCHAR(255),
        address_2 VARCHAR(255),
        city VARCHAR(255),
        state_name VARCHAR(255),
        zip_code VARCHAR(255),
        join_date DATE
        )
        """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(255) PRIMARY KEY,
    product_code VARCHAR(255),
    product_description VARCHAR(255)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    transaction_date DATE,
    product_id VARCHAR(255),
    product_code VARCHAR(255),
    product_description VARCHAR(255),
    quantity INT,
    account_id VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES products (product_id),
    FOREIGN KEY (account_id) REFERENCES accounts (customer_id)
    )
    """)
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_product_id ON transactions (product_id)""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_account_id ON transactions (account_id)""")


    def process_date_format(file):
        with open(file, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            print(f"{file} columns: {reader.fieldnames}")
            print(f"{file} column count: {len(reader.fieldnames)}")

            # modify the date format
            for r in reader:
                print(len(r))
                r = {k.strip(): v.strip() for k, v in r.items()}
                for k,v in r.items():
                    if 'date' in k:
                        r[k] = datetime.strptime(v.strip(), '%Y/%m/%d').strftime('%Y-%m-%d')

                cur.execute(f"INSERT INTO {Path(file).stem} VALUES ({', '.join(['%s']*len(r))})",
                            list(r.values())
                            )


    process_date_format("data/accounts.csv")
    process_date_format('data/products.csv')
    process_date_format('data/transactions.csv')

    conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
