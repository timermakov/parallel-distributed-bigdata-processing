# parallel-distributed-bigdata-processing
Tasks from Parallel and distributed big data processing ITMO class

---

## Общие требования
- Python >=3.10
- Poetry
- Docker + Docker Compose (для Spark-кластера)

## Установка
### Локальная установка
1. Создать виртуальное окружение и установить зависимости:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# или source venv/bin/activate  # Linux/Mac
pip install poetry
poetry install --no-root
```

2. Запустить тесты:
```bash
pytest -v
```

---

## Lab #1: PySpark, N-th Fibonacci number

Вычисление N-го числа Фибоначчи с использованием PySpark в режиме клиента (`--deploy-mode=client`).

### Структура
- `src/lab1/fibonacci.py` — алгоритм fast doubling
- `src/lab1/main.py` — точка входа, CLI
- `tests/lab1/test_fibonacci.py` — pytest-тесты

### Локальный запуск

**Вариант 1: Локально без Docker (local[*])**
```bash
python -m lab1.main --n 10 --master local[*]
```

**Вариант 2: Через spark-submit локально**
```bash
spark-submit --master local[*] --deploy-mode client src/lab1/main.py --n 10
```

**Вариант 3: Подключение к Docker Spark кластеру**
```bash
# Запустить кластер
docker compose up -d spark-master spark-worker

# Запустить задачу
spark-submit --master spark://localhost:7077 --deploy-mode client src/lab1/main.py --n 10
```

### Запуск через Docker Compose
```bash
# 1. Поднять кластер
docker compose up -d spark-master spark-worker

# 2. Выполнить задачу из контейнера (Linux/Mac)
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab1/main.py --n 10

# 2. Выполнить задачу из контейнера (Windows PowerShell)
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab1/main.py --n 10

# 3. Остановить кластер
docker compose down
```

---

## Lab #2: E-Commerce Data Analytics with PySpark

Анализ данных о продажах британской онлайн-компании с использованием PySpark.

### Структура
- `src/lab2/data_loader.py` — загрузка данных из Kaggle и преобразование в Parquet
- `src/lab2/EDA.py` — exploratory data analysis
- `src/lab2/analytics.py` — бизнес-аналитика (топ товары, статистика клиентов)
- `src/lab2/main.py` — точка входа с CLI
- `tests/lab2/` — тесты

### Dataset
- **Источник**: [E-Commerce Data (Kaggle)](https://www.kaggle.com/datasets/carrie1/ecommerce-data/data)
- **Описание**: Транзакционные данные онлайн-ритейла (2010-2011)
- **Колонки**: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

### Задачи
1. Загрузка датасета через kagglehub, приведение типов, сохранение в Parquet
2. Топ-5 самых популярных товаров по количеству проданных единиц
3. Для каждого клиента: общее число заказов, общая потраченная сумма, средний чек

### Локальный запуск

**1. Загрузка и обработка данных**
```bash
python -m main --task download --data-path data/ecommerce.parquet
```

**2. Exploratory Data Analysis (EDA)**
```bash
python -m main --task eda --data-path data/ecommerce.parquet
```

**3. Запуск аналитики (топ товары + статистика клиентов)**
```bash
python -m main --task analytics --data-path data/ecommerce.parquet --top-n 5 --output-dir results
```

**4. Запуск полного пайплайна (download → EDA → analytics)**
```bash
python -m main --task all --data-path data/ecommerce.parquet --top-n 5
```

### Запуск через Docker Compose

**1. Поднять Spark кластер**
```bash
docker compose up -d spark-master spark-worker
```

**2. Скачать и обработать данные (Linux/Mac)**
```bash
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab2/main.py --task download --data-path /app/data/ecommerce.parquet
```

**2. Скачать и обработать данные (Windows PowerShell)**
```powershell
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab2/main.py --task download --data-path /app/data/ecommerce.parquet
```

**3. Запустить аналитику (Linux/Mac)**
```bash
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab2/main.py --task analytics --data-path /app/data/ecommerce.parquet --output-dir /app/results
```

**3. Запустить аналитику (Windows PowerShell)**
```powershell
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab2/main.py --task analytics --data-path /app/data/ecommerce.parquet --output-dir /app/results
```

**4. Остановить кластер**
```bash
docker compose down
```

### Результаты
После выполнения аналитики результаты сохраняются в директории `results/`:
- `top_products.parquet` / `top_products.csv` — топ-5 товаров
- `customer_statistics.parquet` / `customer_statistics.csv` — статистика по клиентам

---

## Lab #3: Term Frequency Analysis with PySpark

Подсчёт топ-10 слов с максимальными значениями TF (term frequency) для объединённого текста.

### Структура
- `src/lab3/tf.py` — функции токенизации и расчёта TF
- `src/lab3/main.py` — CLI для запуска расчётов
- `tests/lab3/test_tf.py` — модульные тесты

### Запуск

```
python -m lab3.main --input data/AllCombined.txt --top 10 --min-length 4 --master local[*]
```

Опции:
- `--top` — количество слов в выдаче (по умолчанию 10)
- `--min-length` — минимальная длина учитываемых слов (по умолчанию 4)
- `--output` — путь к директории для сохранения результатов (CSV или Parquet, см. `--format`)
- `--format` — формат сохранения (`csv` или `parquet`, по умолчанию `csv`)

Пример сохранения:

```
python -m lab3.main --output results/lab3/top_tf_words --format csv
```

### Запуск c Docker Compose

**1. Поднять Spark**
```bash
docker compose up -d spark-master spark-worker
```

**2. Выполнить задачу**

**Linux/Mac**
```bash
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab3/main.py --input /app/data/AllCombined.txt --top 10 --min-length 4 \
  --output /app/results/lab3/top_tf_words --format csv
```
**Windows PowerShell**
```powershell
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client `
  src/lab3/main.py --input /app/data/AllCombined.txt --top 10 --min-length 4 `
  --output /app/results/lab3/top_tf_words --format csv
```

**3. Остановить**
```bash
docker compose down
```

---

## Lab #4: NBA Player Efficiency Analytics with PySpark

Анализ эффективности баскетбольных игроков NBA и их соотношения цена/эффективность с использованием PySpark.

### Структура
- `src/lab4/data_loader.py` — загрузка данных из CSV файлов (players, salaries, season stats)
- `src/lab4/processor.py` — расчёт эффективности и cost per efficiency, сохранение в партиционированный Parquet
- `src/lab4/analytics.py` — поиск топ-5 наиболее выгодных игроков за каждый сезон
- `src/lab4/main.py` — точка входа с CLI
- `tests/lab4/` — тесты

### Dataset
Необходимо вручную разместить три CSV файла в директории `data/lab4/`:
- **players.csv**: данные по баскетбольным игрокам (id и полное имя)
- **salaries_1985to2018.csv**: стоимость игрока за конкретный сезон (1985-2018)
- **Seasons_Stats.csv**: статистика игрока в конкретном сезоне (PTS, TRB, AST и др.)

### Задачи
1. Расчёт эффективности игрока: `Efficiency = PTS + TRB + AST`
   - PTS — очки, набранные игроком
   - TRB — количество подборов
   - AST — количество передач (assists)

2. Расчёт стоимости за единицу эффективности: `Cost per Efficiency = Salary / Efficiency`

3. Сохранение результата в Parquet с партиционированием по годам (partition discovery)

4. Вывод топ-5 наиболее выгодных игроков (с наименьшим Cost per Efficiency) за каждый год

### Особенности реализации
- Сезон NBA указывается в формате "1990-91" — используется год окончания (1991)
- Фильтрация записей с нулевой или отрицательной эффективностью
- Использование Window функций для ранжирования игроков по сезонам

### Локальный запуск

```bash
cd src/lab4
```

**1. Обработка данных (расчёт эффективности и сохранение в Parquet)**
```bash
python -m main --task process \
  --players ../../data/lab4/players.csv \
  --salaries ../../data/lab4/salaries_1985to2018.csv \
  --seasons-stats ../../data/lab4/Seasons_Stats.csv \
  --processed-path ../../data/lab4/processed.parquet
```

**2. Запуск аналитики (топ-5 игроков за каждый сезон)**
```bash
python -m main --task analytics \
  --processed-path ../../data/lab4/processed.parquet \
  --top-n 5
```

**3. Запуск полного пайплайна (обработка + аналитика)**
```bash
python -m main --task all \
  --players ../../data/lab4/players.csv \
  --salaries ../../data/lab4/salaries_1985to2018.csv \
  --seasons-stats ../../data/lab4/Seasons_Stats.csv \
  --processed-path ../../data/lab4/processed.parquet \
  --top-n 5
```

### Запуск через Docker Compose

**1. Поднять Spark кластер**
```bash
docker compose up -d spark-master spark-worker
```

**2. Обработать данные (Linux/Mac)**
```bash
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab4/main.py --task process \
  --players /app/data/lab4/players.csv \
  --salaries /app/data/lab4/salaries_1985to2018.csv \
  --seasons-stats /app/data/lab4/Seasons_Stats.csv \
  --processed-path /app/data/lab4/processed.parquet
```

**2. Обработать данные (Windows PowerShell)**
```powershell
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab4/main.py --task process --players /app/data/lab4/players.csv --salaries /app/data/lab4/salaries_1985to2018.csv --seasons-stats /app/data/lab4/Seasons_Stats.csv --processed-path /app/data/lab4/processed.parquet
```

**3. Запустить аналитику (Linux/Mac)**
```bash
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab4/main.py --task analytics \
  --processed-path /app/data/lab4/processed.parquet \
  --top-n 5
```

**3. Запустить аналитику (Windows PowerShell)**
```powershell
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab4/main.py --task analytics --processed-path /app/data/lab4/processed.parquet --top-n 5
```

**4. Запустить полный пайплайн (Linux/Mac)**
```bash
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab4/main.py --task all \
  --players /app/data/lab4/players.csv \
  --salaries /app/data/lab4/salaries_1985to2018.csv \
  --seasons-stats /app/data/lab4/Seasons_Stats.csv \
  --processed-path /app/data/lab4/processed.parquet \
  --top-n 5
```

**4. Запустить полный пайплайн (Windows PowerShell)**
```powershell
docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab4/main.py --task all --players /app/data/lab4/players.csv --salaries /app/data/lab4/salaries_1985to2018.csv --seasons-stats /app/data/lab4/Seasons_Stats.csv --processed-path /app/data/lab4/processed.parquet --top-n 5
```

**5. Остановить кластер**
```bash
docker compose down
```

### Результаты
После выполнения обработки данные сохраняются в партиционированный Parquet:
- `data/lab4/processed.parquet/season_year=1985/` — данные за нужный год для всех доступных сезонов

Результаты аналитики выводятся в консоль в виде таблицы с топ-5 игроками за каждый сезон.

---

## Общая структура проекта
```
.
├── config/
│   └── config.py              # Конфигурация SparkSession
├── src/
│   ├── lab1/                  # Lab 1: Fibonacci
│   │   ├── fibonacci.py       # Алгоритм fast doubling
│   └   └── main.py            # CLI для Lab 1
│   ├── lab2/                  # Lab 2: E-Commerce Analytics
│   │   ├── data_loader.py     # Загрузка и обработка данных
│   │   ├── EDA.py             # Exploratory Data Analysis
│   │   ├── analytics.py       # Бизнес-аналитика
│   │   └── main.py            # CLI для Lab 2
│   ├── lab3/                  # Lab 3: Term Frequency Analysis
│   │   ├── tf.py              # Подсчёт частоты встречаемых слов
│   │   └── main.py            # CLI для Lab 3
│   └── lab4/                  # Lab 4: NBA Player Efficiency Analytics
│       ├── data_loader.py     # Загрузка CSV датасетов
│       ├── processor.py       # Расчёт эффективности и cost/efficiency
│       ├── analytics.py       # Поиск топ-5 игроков
│       └── main.py            # CLI для Lab 4
├── tests/
│   ├── lab1/                  # Тесты Lab 1
│   ├── lab2/                  # Тесты Lab 2
│   ├── lab3/                  # Тесты Lab 3
│   └── lab4/                  # Тесты Lab 4
├── docker-compose.yml         # Spark кластер (master + worker + client)
├── pyproject.toml             # Зависимости проекта
└── README.md                  # Документация
```

---

## Docker Spark Cluster
Кластер состоит из:
- **spark-master**: Мастер-нода (порты 8080, 7077)
- **spark-worker**: Воркер-нода (порт 8081)
- **client**: Клиентский контейнер для запуска задач

Веб-интерфейсы:
- Master UI: http://localhost:8080
- Worker UI: http://localhost:8081

---

## Kaggle API Setup
Для загрузки данных через kagglehub необходим Kaggle API token:

1. Создайте аккаунт на [Kaggle](https://www.kaggle.com)
2. Перейдите в настройки: Account → API → Create New API Token
3. Сохраните `kaggle.json` в `~/.kaggle/` (Linux/Mac) или `%USERPROFILE%\.kaggle\` (Windows)
4. Установите права доступа (Linux/Mac): `chmod 600 ~/.kaggle/kaggle.json`

Альтернативно, установите переменные окружения:
```bash
export KAGGLE_USERNAME="your-username"
export KAGGLE_KEY="your-api-key"
```

---

## Примечания
- Lab 1: Fast doubling алгоритм O(log n) для вычисления чисел Фибоначчи
- Lab 2: Распределённая обработка данных с PySpark (оптимизация для больших объёмов)
- Lab 3: Анализ частоты встречаемых слов (Term Frequency) с использованием PySpark
- Lab 4: Анализ эффективности игроков NBA
- Все вычисления выполняются в client mode
- Результаты сохраняются в Parquet
