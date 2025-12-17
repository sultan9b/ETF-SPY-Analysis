import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "sonador"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "spy_etf"

# S3 / MinIO
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# Alpha Vantage API
ALPHA_VANTAGE_API_KEY = Variable.get("alpha_vantage_api_key")  # Добавьте в Airflow Variables

# Конфигурация для SPY ETF
SYMBOL = "SPY"  # S&P 500 ETF
OUTPUT_SIZE = "full"  # "compact" (100 последних) или "full" (20+ лет)

LONG_DESCRIPTION = """
# SPY ETF Daily Stock Data Pipeline

Этот DAG загружает ежедневные данные по SPY ETF (S&P 500) 
из Alpha Vantage API и сохраняет их в MinIO/S3 в формате Parquet.

## Источник данных
- API: Alpha Vantage Time Series Daily Adjusted
- Символ: SPY (SPDR S&P 500 ETF Trust)
- Данные: OHLCV + adjusted close + dividend + split
- История: полная (20+ лет данных)

## Поля данных
- timestamp: дата торгов
- open: цена открытия
- high: максимальная цена
- low: минимальная цена  
- close: цена закрытия
- adjusted_close: скорректированная цена закрытия
- volume: объем торгов
- dividend_amount: дивиденды
- split_coefficient: коэффициент сплита

## Выходные данные
- Формат: Parquet (сжатие GZIP)
- Расположение: s3://prod/raw/spy_etf/YYYY-MM-DD/

## Ограничения API
- Free tier: 25 запросов в день
- Premium: 75+ запросов в день
"""

SHORT_DESCRIPTION = "Загрузка котировок SPY ETF из Alpha Vantage API в S3"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Almaty"),
    "catchup": False,  # Не нужен catchup - каждый раз загружаем полную историю
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """Получить даты из контекста Airflow"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """
    Загрузить данные по SPY ETF из Alpha Vantage API
    и сохранить в MinIO/S3 в формате Parquet
    """

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for date: {start_date}")

    # Построить URL для Alpha Vantage API
    api_url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={SYMBOL}"
        f"&outputsize={OUTPUT_SIZE}"
        f"&apikey={ALPHA_VANTAGE_API_KEY}"
        f"&datatype=csv"
    )

    logging.info(f"📡 Fetching {SYMBOL} ETF data from Alpha Vantage API")
    logging.info(f"🔗 API URL: {api_url.replace(ALPHA_VANTAGE_API_KEY, '***')}")

    # Подключиться к DuckDB
    con = duckdb.connect()

    try:
        # Путь в S3
        s3_path = f"s3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet"

        logging.info(f"📤 Processing and uploading data to S3: {s3_path}")

        # DuckDB читает CSV из Alpha Vantage и сохраняет в S3
        con.sql(f"""
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            COPY (
                SELECT
                    '{start_date}' as load_date,
                    '{SYMBOL}' as symbol,
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    adjusted_close,
                    volume,
                    dividend_amount,
                    split_coefficient
                FROM read_csv_auto('{api_url}')
            ) TO '{s3_path}'
            (FORMAT PARQUET, COMPRESSION GZIP);
        """)

        logging.info(f"✅ Data successfully saved to S3: {s3_path}")

        # Показать статистику загруженных данных
        result = con.sql(f"""
            SELECT 
                COUNT(*) as total_rows,
                MIN(timestamp) as earliest_date,
                MAX(timestamp) as latest_date,
                ROUND(AVG(close), 2) as avg_close_price,
                ROUND(MIN(close), 2) as min_close_price,
                ROUND(MAX(close), 2) as max_close_price,
                ROUND(SUM(volume), 0) as total_volume
            FROM read_csv_auto('{api_url}')
        """).fetchone()

        logging.info(f"📊 {SYMBOL} ETF Data Statistics:")
        logging.info(f"   Total trading days: {result[0]}")
        logging.info(f"   Date range: {result[1]} to {result[2]}")
        logging.info(f"   Average close price: ${result[3]}")
        logging.info(f"   Price range: ${result[4]} - ${result[5]}")
        logging.info(f"   Total volume: {result[6]:,.0f}")

    except Exception as e:
        logging.error(f"❌ Error processing data: {str(e)}")

        # Проверить возможные причины ошибки
        if "API call frequency" in str(e) or "premium" in str(e).lower():
            logging.error("⚠️ API rate limit exceeded. Free tier allows 25 requests/day.")
        elif "Invalid API call" in str(e):
            logging.error("⚠️ Check if API key is valid and set correctly in Airflow Variables.")

        raise
    finally:
        con.close()

    logging.info(f"✅ Download for date success: {start_date}")


# Определение DAG
with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 2 * * *",  # Каждый день в 02:00 UTC (после закрытия US рынка)
        default_args=args,
        tags=["s3", "raw", "stocks", "spy", "etf", "alpha-vantage"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3_task = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3_task >> end
