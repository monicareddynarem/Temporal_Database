import psycopg2

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="23CS10059",
            user="23CS10059",
            password="ashok@123",
            host="10.5.18.102",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
"""
def get_db_connection():
    conn = psycopg2.connect(
        host = 'localhost',
        database = 'temporaldb',
        user='postgres',
        password='Avinash6174',
        port='5432'
    )
    return conn
"""