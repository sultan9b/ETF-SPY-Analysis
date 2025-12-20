import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = "sonador"
DAG_ID = "raw_unemployment_from_s3_to_pg"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "unemployment_owid"
SCHEMA = "ods"
TARGET_TABLE = "fct_unemployment"

# S3 / MinIO
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# PostgreSQL
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# Unemployment Data to PostgreSQL Pipeline

Этот DAG загружает данные об уровне безработицы из MinIO/S3 
и сохраняет их в PostgreSQL DWH.

## Источник данных
- Источник: MinIO bucket prod
- Путь: raw/unemployment_owid/YYYY-MM-DD/*.parquet
- Формат: Parquet (сжатие GZIP)

## Целевая таблица
- База данных: postgres_dwh
- Схема: ods
- Таблица: fct_unemployment

## Поля таблицы
- load_date: дата загрузки
- entity: название страны
- code: код страны (ISO)
- year: год
- unemployment_rate: уровень безработицы (%)

## Зависимости
- Ожидает выполнения DAG: raw_unemployment_from_owid_to_s3
"""

SHORT_DESCRIPTION = "Загрузка данных об уровне безработицы из S3 в PostgreSQL DWH"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Almaty"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """Получить даты из контекста Airflow"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def create_table_if_not_exists(**context):
    """
    Создать таблицу в PostgreSQL если её не существует
    """

    logging.info("🔧 Creating table if not exists...")
    con = duckdb.connect()

    try:
        con.sql(f"""
            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE postgres,
                USER 'postgres',
                PASSWORD '{PASSWORD}'
            );

            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

            CREATE SCHEMA IF NOT EXISTS dwh_postgres_db.{SCHEMA};

            CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
                load_date DATE NOT NULL,
                entity VARCHAR(255) NOT NULL,
                code VARCHAR(10),
                year INTEGER NOT NULL,
                unemployment_rate DECIMAL(10,6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (entity, year, load_date)
            );
        """)

        logging.info(f"✅ Table {SCHEMA}.{TARGET_TABLE} is ready")

    except Exception as e:
        logging.error(f"❌ Error creating table: {str(e)}")
        raise
    finally:
        con.close()


def get_and_transfer_raw_data_to_ods_pg(**context):
    """
    Загрузить данные из MinIO/S3 в PostgreSQL DWH
    """

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()

    try:
        # Путь к данным в S3
        s3_path = f"s3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet"

        logging.info(f"📥 Reading data from: {s3_path}")

        con.sql(f"""
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE postgres,
                USER 'postgres',
                PASSWORD '{PASSWORD}'
            );

            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """)

        # Проверить сколько записей в файле
        count_result = con.sql(f"""
            SELECT COUNT(*) as total_rows
            FROM '{s3_path}'
        """).fetchone()

        logging.info(f"📊 Found {count_result[0]} rows in source file")

        # Удалить старые данные за эту дату загрузки (если есть)
        con.sql(f"""
            DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE load_date = '{start_date}';
        """)

        logging.info(f"🗑️ Deleted old data for date: {start_date}")

        # Вставить новые данные
        con.sql(f"""
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                load_date,
                entity,
                code,
                year,
                unemployment_rate
            )
            SELECT
                load_date,
                Entity as entity,
                Code as code,
                Year as year,
                "Unemployment, total (% of total labor force) (modeled ILO estimate)" as unemployment_rate
            FROM '{s3_path}'
            WHERE Year IS NOT NULL;
        """)

        # Проверить сколько записей вставлено
        inserted_count = con.sql(f"""
            SELECT COUNT(*) as inserted_rows
            FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE load_date = '{start_date}';
        """).fetchone()

        logging.info(f"✅ Inserted {inserted_count[0]} rows into PostgreSQL")

        # Показать статистику по странам
        stats = con.sql(f"""
            SELECT 
                entity,
                COUNT(*) as years_count,
                MIN(year) as min_year,
                MAX(year) as max_year,
                ROUND(AVG(unemployment_rate), 2) as avg_rate
            FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE load_date = '{start_date}'
            GROUP BY entity
            ORDER BY entity;
        """).fetchall()

        logging.info(f"📈 Statistics by country:")
        for row in stats:
            logging.info(f"   {row[0]}: {row[1]} years ({row[2]}-{row[3]}), avg: {row[4]}%")

    except Exception as e:
        logging.error(f"❌ Error transferring data: {str(e)}")
        raise
    finally:
        con.close()

    logging.info(f"✅ Transfer for date success: {start_date}")


# Определение DAG
with DAG(
        dag_id=DAG_ID,
        schedule_interval="@monthly",  # Раз в месяц, синхронно с загрузкой из OWID
        default_args=args,
        tags=["s3", "ods", "pg", "unemployment", "dwh"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    # Сенсор ожидает выполнения DAG загрузки из OWID
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_unemployment_from_owid_to_s3",
        external_task_id="end",  # Ожидать завершения задачи "end"
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,  # 1 час
        poke_interval=60,  # проверять каждую минуту
    )

    # Создать таблицу если не существует
    create_table_task = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    # Загрузить данные из S3 в PostgreSQL
    transfer_data_task = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> create_table_task >> transfer_data_task >> end