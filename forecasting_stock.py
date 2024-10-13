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


@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
    - Create a view with training related columns
    - Create a model with the view above
    """

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE PROCEDURE {forecast_function_name}()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
    var result = SYSTEM$REFERENCE('VIEW', '{train_view}');
    var forecast_model = {forecast_function_name};
    return forecast_model;
    $$;"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model.
        cur.execute(f"CALL {forecast_function_name}();")
    except Exception as e:
        print(e)
        raise


@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
    - Generate predictions and store the results to a table named forecast_table.
    - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {forecast_function_name}(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""
    
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise


default_args = {
   'owner': 'ariel',
   'depends_on_past': False,
   'email': ['ariel.hsieh@sjsu.edu'],
   'retries': 1,
   'retry_delay': timedelta(minutes=3),
}


with DAG(
    dag_id = 'TrainPredict',
    start_date = datetime(2024,10,12),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '12 23 * * *',
    default_args=default_args
) as dag:

    train_input_table = "stock_price_db.raw_data.stock_price"
    train_view = "stock_price_db.adhoc.stock_price_view"
    forecast_table = "stock_price_db.adhoc.stock_price_forecast"
    forecast_function_name = "stock_price_db.analytics.predict_stock_price"
    final_table = "stock_price_db.analytics.stock_price"
    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)
