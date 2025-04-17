import configparser
import psycopg2
from sql_queries import copy_table_queries


def staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            print(f"Running COPY query:\n{query[:100]}...")  # print first 100 chars
            cur.execute(query)
            conn.commit()
            print("✅ COPY successful.")
        except Exception as e:
            print("❌ COPY failed!")
            print(f"Error: {e}")
            conn.rollback()  # optionally rollback if something fails

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    staging_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()