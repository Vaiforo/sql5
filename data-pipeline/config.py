from dotenv import load_dotenv
import os

load_dotenv()

# DB_CONN_STR = os.getenv("DB_CONN_STR")
DB_CONN_STR = "host=127.0.0.1 port=5432 dbname=auto-seller user=postgres password=ivas55646"
AUTH_KEY = os.getenv("AUTH_KEY")
