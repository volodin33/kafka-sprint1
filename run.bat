@echo off
setlocal

set "KAFKA_COMPOSE=%~dp0docker-compose.yml"
set "NET_COMPOSE=%~dp0csharp\docker-compose.app.yml"
set "JAVA_COMPOSE=%~dp0java\docker-compose.app.yml"

if /i "%~1"=="net" set "APP_COMPOSE=%NET_COMPOSE%"
if /i "%~1"=="java" set "APP_COMPOSE=%JAVA_COMPOSE%"
if not defined APP_COMPOSE (
    echo Использование: 'run.bat net' или 'run.bat java'
    exit /b 1
)

echo --- Остановка приложений ---
if exist "%NET_COMPOSE%" docker compose -f "%NET_COMPOSE%" down
if exist "%JAVA_COMPOSE%" docker compose -f "%JAVA_COMPOSE%" down

echo --- Удаление кластера ---
docker compose -f "%KAFKA_COMPOSE%" down -v

echo --- Запуск Kafka ---
docker compose -f "%KAFKA_COMPOSE%" up -d

echo --- Ожидание брокеров (20 сек) ---
ping -n 20 127.0.0.1 >nul

echo --- Создание топика orders ---
docker exec kafka-0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0:9092 --create --topic orders --partitions 3 --replication-factor 3 --config min.insync.replicas=2

docker exec kafka-0 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0:9092 --describe --topic orders

echo --- Запуск приложения (%~1) (Ctrl+C - остановка) ---
docker compose -f "%APP_COMPOSE%" up --build

echo --- Приложение остановлено ---
docker compose -f "%APP_COMPOSE%" ps

