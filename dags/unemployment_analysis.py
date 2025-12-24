import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

# Конфигурация DAG
OWNER = "sonador"
DAG_ID = "dm_unemployment_analysis"

# Используемые таблицы в DAG
SCHEMA = "dm"
TARGET_TABLE = "unemployment_analysis"
SOURCE_SCHEMA = "ods"
SOURCE_TABLE = "fct_unemployment"

# DWH
PG_CONNECT = "postgres_dwh"

LONG_DESCRIPTION = """
# Витрина данных: Анализ безработицы

Эта витрина содержит актуальные данные по уровню безработицы 
для всех стран с дополнительными аналитическими метриками.

## Источник данных:
- Схема: ods
- Таблица: fct_unemployment

## Структура витрины:
- entity: Название страны/региона
- code: Код страны (ISO)
- year: Год данных
- unemployment_rate: Уровень безработицы (%)
- unemployment_category: Категория (Низкая/Умеренная/Высокая)
- prev_year_rate: Значение за предыдущий год
- year_over_year_change: Изменение год к году
- trend_direction: Направление тренда (Улучшение/Ухудшение/Стабильно)
- global_avg_year: Среднее значение по миру за год
- diff_from_global_avg: Отклонение от среднемирового
- rank_by_year: Рейтинг страны по безработице в году
- percentile_by_year: Процентиль
- source_load_date: Дата загрузки данных в систему
- dm_refresh_date: Дата обновления витрины

## Логика обновления:
- Берется последняя версия данных для каждой страны и года
- Рассчитываются аналитические метрики
- Полная перезагрузка витрины при каждом запуске
"""

SHORT_DESCRIPTION = "Обновление витрины данных по безработице с аналитическими метриками"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Almaty"),
    "catchup": False,  # Для витрины лучше False
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=30),
}

with DAG(
        dag_id=DAG_ID,
        schedule_interval="@monthly", #"0 2 1 * *",  # 1-го числа каждого месяца в 2:00
        default_args=args,
        tags=["dm", "unemployment", "analytics", "data-mart"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

  # Сенсор ожидает выполнения DAG загрузки в ODS
    sensor_on_ods_layer = ExternalTaskSensor(
        task_id="sensor_on_ods_layer",
        external_dag_id="raw_unemployment_from_s3_to_pg",
        external_task_id="end",
        execution_date_fn=lambda exec_date: exec_date,
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60
    )

    # ОДНА ЗАДАЧА для UPSERT данных в витрину
    # Эта задача выполняет всю логику: выбор последних данных, расчет метрик и вставку/обновление.
    upsert_into_dm = SQLExecuteQueryOperator(
        task_id="upsert_into_dm",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql="""
        -- Основная операция UPSERT для витрины dm.unemployment_analysis
        WITH latest_data AS (
            SELECT DISTINCT ON (entity, year)
                entity,
                code,
                year,
                unemployment_rate,
                load_date as source_load_date
            FROM ods.fct_unemployment
            WHERE unemployment_rate IS NOT NULL
            ORDER BY entity, year, load_date DESC
        ),
        analytics AS (
            SELECT 
                ld.*,
                LAG(ld.unemployment_rate, 1) OVER (PARTITION BY ld.entity ORDER BY ld.year) as prev_year_rate,
                AVG(ld.unemployment_rate) OVER (PARTITION BY ld.year) as global_avg_year,
                RANK() OVER (PARTITION BY ld.year ORDER BY ld.unemployment_rate DESC) as rank_by_year,
                PERCENT_RANK() OVER (PARTITION BY ld.year ORDER BY ld.unemployment_rate) as percentile_by_year
            FROM latest_data ld
        ),
        final_data AS (
            SELECT 
                a.entity, a.code, a.year, a.unemployment_rate,
                a.source_load_date,
                CASE 
                    WHEN a.unemployment_rate < 4 THEN 'Very Low'
                    WHEN a.unemployment_rate < 8 THEN 'Low'
                    WHEN a.unemployment_rate < 12 THEN 'Moderate'
                    WHEN a.unemployment_rate < 20 THEN 'High'
                    ELSE 'Very High'
                END as unemployment_category,
                a.prev_year_rate,
                (a.unemployment_rate - a.prev_year_rate) as year_over_year_change,
                CASE 
                    WHEN a.prev_year_rate IS NOT NULL THEN
                        CASE 
                            WHEN a.unemployment_rate < a.prev_year_rate THEN 'Improving'
                            WHEN a.unemployment_rate > a.prev_year_rate THEN 'Worsening'
                            ELSE 'Stable'
                        END
                    ELSE 'No previous data'
                END as trend_direction,
                a.global_avg_year,
                (a.unemployment_rate - a.global_avg_year) as diff_from_global_avg,
                a.rank_by_year,
                a.percentile_by_year
            FROM analytics a
        )
        INSERT INTO dm.unemployment_analysis (
            entity, code, year, unemployment_rate, source_load_date,
            unemployment_category, prev_year_rate, year_over_year_change,
            trend_direction, global_avg_year, diff_from_global_avg,
            rank_by_year, percentile_by_year, dm_refresh_date, updated_at
        )
        SELECT 
            fd.entity, fd.code, fd.year, fd.unemployment_rate, fd.source_load_date,
            fd.unemployment_category, fd.prev_year_rate, fd.year_over_year_change,
            fd.trend_direction, fd.global_avg_year, fd.diff_from_global_avg,
            fd.rank_by_year, fd.percentile_by_year, CURRENT_DATE, CURRENT_TIMESTAMP
        FROM final_data fd
        ON CONFLICT (entity, year, source_load_date) 
        DO UPDATE SET
            unemployment_rate = EXCLUDED.unemployment_rate,
            unemployment_category = EXCLUDED.unemployment_category,
            prev_year_rate = EXCLUDED.prev_year_rate,
            year_over_year_change = EXCLUDED.year_over_year_change,
            trend_direction = EXCLUDED.trend_direction,
            global_avg_year = EXCLUDED.global_avg_year,
            diff_from_global_avg = EXCLUDED.diff_from_global_avg,
            rank_by_year = EXCLUDED.rank_by_year,
            percentile_by_year = EXCLUDED.percentile_by_year,
            dm_refresh_date = EXCLUDED.dm_refresh_date,
            updated_at = EXCLUDED.updated_at;
        """
    )

    # Задача для обновления материализованных представлений (если они нужны)
    refresh_materialized_views = SQLExecuteQueryOperator(
        task_id="refresh_materialized_views",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql="""
        -- Пример: обновляем одно представление, остальные по аналогии
        REFRESH MATERIALIZED VIEW CONCURRENTLY dm.mv_top_unemployment_countries;
        -- REFRESH MATERIALIZED VIEW CONCURRENTLY dm.mv_unemployment_trends;
        -- REFRESH MATERIALIZED VIEW CONCURRENTLY dm.mv_unemployment_by_category;
        """
    )

    end = EmptyOperator(task_id="end")

    # УПРОЩЕННЫЙ ПОТОК ВЫПОЛНЕНИЯ
    start  >> sensor_on_ods_layer >> upsert_into_dm >> refresh_materialized_views >> end