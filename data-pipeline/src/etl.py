from datetime import date

from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.fill_structured_table import fill_structured_table


def etl():
    n = 20
    start_date = date(2023, 1, 1)
    end_date = date(2025, 12, 31)

    df = get_dataset(n)

    load_data_to_db(df)

    fill_structured_table(start_date, end_date)

    print("[I] ETL завершён")
