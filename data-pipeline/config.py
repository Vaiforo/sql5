from dotenv import load_dotenv
import os

load_dotenv()

DB_CONN_STR = os.getenv("DB_CONN_STR")
AUTH_KEY = os.getenv("AUTH_KEY")
