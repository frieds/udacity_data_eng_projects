import configparser
import psycopg2
from sql_queries import insert_table_queries


def main():
    """
    runs high-level functions to read in AWS variables, connect to db, load staging tables, and insert data into tables for star schema
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect(host=config['CLUSTER']['host'],
                            dbname=config['CLUSTER']['DB_NAME'],
                            user=config['CLUSTER']['DB_USER'],
                            password=config['CLUSTER']['DB_PASSWORD'],
                            port=config['CLUSTER']['DB_PORT']
                           )
    cur = conn.cursor()
    print("cursor object to redshift db created")
    print("lets create fact and dimension tables from the two staging tables...")
    for query in insert_table_queries:
        print("query: {}".format(query))
        cur.execute(query)
        print("query executed")
        conn.commit()
        print("query committed")
    print("data copied from staging tables to fact and dimension tables!")
    conn.close()


if __name__ == "__main__":
    main()