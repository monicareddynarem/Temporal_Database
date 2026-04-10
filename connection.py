import psycopg2

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="2006",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None