import configparser
import psycopg2
from sql_queries import insert_table_queries


def insert(cur, conn):
    for query in insert_table_queries:
        try:
            print(f"Running Inserting query:\n{query[:100]}...")  # print first 100 chars
            cur.execute(query)
            conn.commit()
            print("✅ Insert successful.")
        except Exception as e:
            print("❌ Insert failed!")
            print(f"Error: {e}")
            conn.rollback()  # optionally rollback if something fails

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    insert(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()