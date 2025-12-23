/*создаем схемы*/
create schema stg;
create schema ods;
create schema dm;
 
/*создаем таблицу с ощищенным данными*/
CREATE TABLE IF NOT EXISTS ods.fct_unemployment (
                load_date DATE,
                entity VARCHAR(255),
                code VARCHAR(10),
                year INTEGER,
                unemployment_rate DECIMAL(10,6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

select count(*) from ods.fct_unemployment;




/* создаем структуру таблицы для витрины данных*/
CREATE TABLE IF NOT EXISTS dm.unemployment_analysis (
    record_id BIGSERIAL PRIMARY KEY,
    entity VARCHAR(255) NOT NULL,
    code VARCHAR(10),
    year INTEGER NOT NULL,
    unemployment_rate DECIMAL(10, 6),
    unemployment_category VARCHAR(20),
    prev_year_rate DECIMAL(5, 2),
    year_over_year_change DECIMAL(5, 2),
    trend_direction VARCHAR(35),
    global_avg_year DECIMAL(5, 2),
    diff_from_global_avg DECIMAL(5, 2),
    rank_by_year INTEGER,
    percentile_by_year DECIMAL(5, 3),
    source_load_date DATE,
    dm_refresh_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_entity_year_source UNIQUE (entity, year, source_load_date)
);

/* создадим индексы для ускорения выборок*/
create index if not exists idx_unemp_analysis_entity_year on dm.unemployment_analysis(entity, year desc);
create index if not exists idx_unemp_analysis_refresh_date on dm.unemployment_analysis(dm_refresh_date);
comment on table dm.unemployment_analysis is 'Аналитическая витрина по безработице. Данные обновляются по расписанию через DAG.';



/* materialized представления*/
-- Витрина 1. Топ 50 по безработице за последний год

create materialized view if not exists dm.mv_top_unemployment_countries as
select 
	entity,
	code,
	year,
	unemployment_rate,
	rank_by_year,
	unemployment_category,
	trend_direction
from dm.unemployment_analysis 
where year = (select max(year) from dm.unemployment_analysis)
order by unemployment_rate desc
limit 50;

-- Индекс для быстрого обновления
create unique index if not exists idx_mv_top_countries on dm.mv_top_unemployment_countries(entity, year);


-- Витрина 2. Динамика по годам для топ-10 стран за последние 10 лет

CREATE MATERIALIZED VIEW IF NOT EXISTS dm.mv_unemployment_trends AS
WITH country_stats AS (
    SELECT 
        entity,
        AVG(unemployment_rate) as avg_unemployment_last_5_years
    FROM dm.unemployment_analysis
    WHERE year >= (SELECT MAX(year) - 5 FROM dm.unemployment_analysis)
    GROUP BY entity
),
top_countries AS (
    SELECT 
        entity
    FROM country_stats
    ORDER BY avg_unemployment_last_5_years DESC
    LIMIT 10
)
SELECT 
    d.entity,
    d.code,
    d.year,
    d.unemployment_rate,
    d.trend_direction,
    d.rank_by_year,
    d.unemployment_category
FROM dm.unemployment_analysis d
WHERE d.entity IN (SELECT entity FROM top_countries)
  AND d.year >= (SELECT MAX(year) - 10 FROM dm.unemployment_analysis)
ORDER BY d.entity, d.year;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_trends 
    ON dm.mv_unemployment_trends(entity, year);



-- Витрина 3: Статистика по категориям
CREATE MATERIALIZED VIEW IF NOT EXISTS dm.mv_unemployment_by_category AS
SELECT 
    unemployment_category,
    year,
    COUNT(*) as country_count,
    ROUND(AVG(unemployment_rate), 2) as avg_rate,
    MIN(unemployment_rate) as min_rate,
    MAX(unemployment_rate) as max_rate
FROM dm.unemployment_analysis
GROUP BY unemployment_category, year
ORDER BY year DESC, avg_rate DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_category 
    ON dm.mv_unemployment_by_category(unemployment_category, year);





















