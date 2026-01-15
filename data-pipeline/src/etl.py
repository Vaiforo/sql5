from datetime import date

from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.fill_structured_table import fill_structured_table
from src.fill_dm_table import fill_dm_table
from src.export_dm_to_mysql import export_dm_to_mysql


def etl():
    n = 100
    start_date = date(1995, 1, 1)
    end_date = date(2025, 12, 31)

    # df = get_dataset(n)

    # load_data_to_db(df)

    # fill_structured_table(start_date, end_date)

    fill_dm_table(start_date, end_date)

    export_dm_to_mysql(start_date, end_date)

    print("[I] ETL завершён")
