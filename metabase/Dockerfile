FROM eclipse-temurin:19-jdk

# Создать директории
RUN mkdir -p /home/plugins

# Установить переменные окружения
ENV MB_PLUGINS_DIR=/home/plugins/

# Скачать Metabase
ADD https://downloads.metabase.com/v0.52.13/metabase.jar /home/metabase.jar

# Скачать DuckDB драйвер
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12-b/duckdb.metabase-driver.jar /home/plugins/duckdb.metabase-driver.jar

# Установить права
RUN chmod 644 /home/metabase.jar && \
    chmod 644 /home/plugins/duckdb.metabase-driver.jar

# Порт
EXPOSE 3000

# Рабочая директория
WORKDIR /home

# Запуск
CMD ["java", "-jar", "metabase.jar"]