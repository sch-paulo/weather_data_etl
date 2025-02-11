import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

print(f"Attempting to connect with:")
print(f"Host: {DB_HOST}")
print(f"Database: {DB_NAME}")
print(f"User: {DB_USER}")
print(f"Password: {'*' * len(DB_PASS) if DB_PASS else 'None'}")

def test_database_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            client_encoding='utf8',
            connect_timeout=3
        )
        print("Successfully connected to the database!")
        return True
    except psycopg2.Error as e:
        print(f"PostgreSQL Error: {e}")
        print(f"Error Code: {e.pgcode}")
        print(f"Error Message: {e.pgerror}")
        return False
    except Exception as e:
        print(f"General Error: {type(e).__name__}")
        print(f"Error Details: {str(e)}")
        return False
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

if __name__ == "__main__":
    test_database_connection()