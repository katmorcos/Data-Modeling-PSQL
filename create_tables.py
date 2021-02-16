import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
   
    """
     Establish connection to database
     parameters:
     cur: cursor to the database
     conn: database connection reference
    """
   # - Creates and connects to the sparkifydb
   #- Returns the connection and cursor to sparkifydb
   
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur,conn


def drop_tables(cur, conn):
    """
     Runs all the drop table queries defined in sql_queries.py
     parameters:
     cur: cursor to the database
     conn: database connection reference
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """
     Runs all the create table queries defined in sql_queries.py
     parameters:
     cur: cursor to the database
     conn: database connection reference 
     """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    -This is driver function where all other functions are called
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets
      cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur,conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
