import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return mysql.connector.connect (
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),     
        database=os.getenv('DB_NAME'),
        password=os.getenv('DB_PASS')
    )
    