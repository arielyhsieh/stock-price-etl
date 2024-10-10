from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


symbol = Variable.get('symbol')


@task
def return_last_90d_price(symbol):
    """
    - return the last 90 days of the stock prices of symbol as a list of json strings
    """
    vantage_api_key = Variable.get('vantage_api_key')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url)
    data = r.json()
    results = []   # empty list for now to hold the 90 days of stock info (open, high, low, close, volume, date)
    for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d
        results.append(stock_info)
        # an example of data["Time Series (Daily)"][d] is
        # {'1. open': '117.3500', '2. high': '119.6600', '3. low': '117.2500', '4. close': '117.8700', '5. volume': '286038878', 'date': '2024-09-17'}
    return results


def load(con, records, target_table, symbol):
    try:
        con.execute("BEGIN;")
        con.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
            date DATE PRIMARY KEY,
            open DECIMAL(10, 4),
            high DECIMAL(10, 4),
            low DECIMAL(10, 4),
            close DECIMAL(10, 4),
            volume INT,
            symbol VARCHAR(10)
        )""")
        # load records
        for r in records:
            date = r["date"]
            open = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            volume = r["5. volume"]
            sql = f"INSERT INTO {target_table} (date, open, high, low, close, volume, symbol) VALUES ('{date}', {open}, {high}, {low}, {close}, {volume}, '{symbol}')"
            # print(sql)
            con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


default_args = {
   'owner': 'ariel',
   'depends_on_past': False,
   'email': ['ariel.hsieh@sjsu.edu'],
   'retries': 1,
   'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id = 'StockPrice',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ETL'],
    schedule = '30 9 * * *',
    default_args=default_args
) as dag:
    target_table = "stock_price_db.raw_data.stock_price"
    symbol = Variable.get("symbol")
    cur = return_snowflake_conn()

    records = return_last_90d_price(symbol)
    load(cur, records.result(), target_table, symbol)
