import os
import yfinance as yf
import pandas as pd
import configparser
from datetime import datetime, timedelta

from pgdb import PGDatabase

dirname = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

SALES_PATH = config["Files"]["SALES_PATH"]
COMPANIES = eval(config["Companies"]["COMPANIES"])
DATABASE_CREDS = config["Database"]

sales_df = pd.DataFrame()
if os.path.exists(SALES_PATH):
    sales_df = pd.read_csv(SALES_PATH)
    os.remove(SALES_PATH)

historical_d = {}

for company in COMPANIES:
    historical_d[company] = yf.download(
        company,
        start=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        end=datetime.today().strftime("%Y-%m-%d"),
        interval="1d",
    ).reset_index()


database = PGDatabase(
    host=DATABASE_CREDS["HOST"],
    database=DATABASE_CREDS["DATABASE"],
    user=DATABASE_CREDS["USER"],
    password=DATABASE_CREDS["PASSWORD"],
)

for i, row in sales_df.iterrows():
    query = f"insert into sales (dt, company, transaction_type, amount) VALUES  ('{row["dt"][6:] + '-' + row["dt"][:2] + '-' + row["dt"][3:5]}', '{row['company']}', '{row['transaction_type']}', {row['amount']})"
    database.post(query)

for company, data in historical_d.items():
    for i, row in data.iterrows():
        dict_row = dict(row)
        query = f"insert into stock (dt, company, open, close) VALUES  ('{dict_row[("Date", "")].strftime("%Y-%m-%d")}', '{company}', {dict_row[('Open', company)]}, {dict_row[('Close', company)]})"
        database.post(query)
