from dotenv import load_dotenv
import os

load_dotenv()

DB_CONN_STR = os.getenv("DB_CONN_STR")
AUTH_KEY = os.getenv("AUTH_KEY")


MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
