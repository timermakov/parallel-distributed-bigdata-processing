# parallel-distributed-bigdata-processing
Tasks from Parallel and distributed big data processing ITMO class
---

## Lab #1: PySpark, N-th Fibonacci number. 
Запуск в режиме клиента (`--deploy-mode=client`) локально или в кластере Spark, развёрнутом через Docker Compose. Параметр `N` передаётся как CLI аргумент `--n`.

### Требования
- Python >=3.10
- Poetry
- Docker + Docker Compose (для Spark-кластера)

### Установка (локально)
venv, Poetry и зависимости:
```
python -m venv venv
venv\Scripts\activate 
pip install poetry
poetry install --no-root
```
Запустить тесты:
```
poetry run pytest -v
```

Локальный запуск через spark-submit (client mode):
- На локальной машине без Docker (Spark master = local[*]):
```
python -m pip show pyspark
spark-submit --master local[*] --deploy-mode client src/main.py --n 10
```
- Подключаясь к мастер-ноде из Docker Compose:
```
docker compose up -d spark-master spark-worker
spark-submit --master spark://localhost:7077 --deploy-mode client src/main.py --n 10
```

### Запуск через Docker Compose
1) Поднять кластер Spark (master + worker) и клиент:
```
docker compose up -d spark-master spark-worker
```
2) Выполнить задачу из контейнера клиента в client-режиме (пример с N=10):
```
docker compose run --rm client /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client src/main.py --n 10
```

Остановить кластер:
```
docker compose down
```

### Примечания
- Вычисление использует «fast doubling» (чистая функция, без RDD/DataFrame)
- Число задаётся через CLI

### Структура проекта
- `src/main.py` — точка входа, парсинг аргументов, инициализация SparkSession, вычисление и вывод результата
- `config/config.py` — конфигурация приложения, фабрика `create_spark_session`
- `tests/` — pytest-тесты для функции вычисления
- `docker-compose.yml` — кластер Spark (master, worker) и клиент для `spark-submit`
