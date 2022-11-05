import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
      Description: copy tables from S3 to the cluster

      Arguments:
          cur: Cursor object that belongs to the connection.
          conn: Object connection  
      Returns:
          None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
      Description: transform and load the table to redshift 

      Arguments:
          cur: Cursor object that belongs to the connection.
          conn: Object connection  
      Returns:
          None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
      Description: manages the connection to redshifts cluster 

      Arguments:
          cur: Cursor object that belongs to the connection.
          conn: Object connection  
      Returns:
          None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()