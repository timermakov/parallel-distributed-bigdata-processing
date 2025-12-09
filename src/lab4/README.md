# Lab 4: NBA Player Efficiency Analytics

## Overview
This lab analyzes NBA player efficiency and calculates cost-effectiveness metrics using PySpark.

## Quick Start

### Prerequisites
- Place CSV files in `data/lab4/`:
  - `players.csv`
  - `salaries_1985to2018.csv`
  - `Seasons_Stats.csv`

### Local Execution

From the `src/lab4` directory:

```bash
cd src/lab4

# Full pipeline (recommended for first run)
python -m main --task all \
  --players ../../data/lab4/players.csv \
  --salaries ../../data/lab4/salaries_1985to2018.csv \
  --seasons-stats ../../data/lab4/Seasons_Stats.csv \
  --processed-path ../../data/lab4/processed.parquet \
  --top-n 5
```

### Docker Execution

```bash
# Start cluster
docker compose up -d spark-master spark-worker

# Run full pipeline (Linux/Mac)
docker compose run --rm client spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  src/lab4/main.py --task all \
  --players /app/data/lab4/players.csv \
  --salaries /app/data/lab4/salaries_1985to2018.csv \
  --seasons-stats /app/data/lab4/Seasons_Stats.csv \
  --processed-path /app/data/lab4/processed.parquet \
  --top-n 5

# Stop cluster
docker compose down
```

## Architecture

### Data Flow
1. **Load** → Read 3 CSV datasets
2. **Transform** → Extract season years, join datasets
3. **Calculate** → Compute efficiency (PTS + TRB + AST) and cost per efficiency
4. **Partition** → Save to Parquet partitioned by season_year
5. **Analyze** → Use Window functions to rank players per season
6. **Display** → Show top 5 most cost-effective players

### Key Features
- **Season Year Extraction**: Parses "1990-91" format to extract ending year (1991)
- **Data Validation**: Filters null values and zero/negative efficiency
- **Partition Discovery**: Enables efficient year-based queries
- **Window Functions**: Efficient ranking within partitions

## Metrics

### Efficiency Formula
```
Efficiency = PTS + TRB + AST
```
Where:
- **PTS**: Points scored
- **TRB**: Total rebounds
- **AST**: Assists

### Cost per Efficiency Formula
```
Cost per Efficiency = Salary / Efficiency
```

Lower value = more cost-effective player

## Module Structure

```
src/lab4/
├── __init__.py
├── config/
│   ├── __init__.py
│   └── config.py          # Spark session configuration
├── data_loader.py         # CSV loading with schema validation
├── processor.py           # Data transformation and efficiency calculations
├── analytics.py           # Top players analysis with Window functions
└── main.py               # CLI entry point
```

## Testing

```bash
# Run all lab4 tests
pytest tests/lab4/ -v

# Run specific test file
pytest tests/lab4/test_processor.py -v
pytest tests/lab4/test_analytics.py -v
```

## Output

### Processed Data
```
data/lab4/processed.parquet/
├── season_year=1985/
├── season_year=1986/
├── ...
└── season_year=2018/
```

### Analytics Output
Console table showing top 5 players per season with columns:
- Rank
- Player Name
- Team
- Salary
- PTS, TRB, AST
- Efficiency
- Cost per Efficiency

## Performance Considerations

1. **Partitioning**: Data is partitioned by `season_year` for efficient filtering
2. **Join Strategy**: Inner join ensures only complete records (salary + stats)
3. **Filtering**: Early filtering of invalid records reduces computation
4. **Window Functions**: Efficient for per-partition ranking without shuffle

## Common Issues

### Missing CSV Files
Ensure all three CSV files are in `data/lab4/` before running process task.

### Column Name Mismatches
The code handles both "Season" and "Year" column names in season stats.

### Division by Zero
Records with efficiency = 0 are automatically filtered out.

## Additional Options

```bash
# Save analytics results to file
python -m main --task analytics \
  --processed-path ../../data/lab4/processed.parquet \
  --output ../../results/lab4/top_players.parquet

# Change number of top players
python -m main --task analytics \
  --processed-path ../../data/lab4/processed.parquet \
  --top-n 10

# Use different Spark master
python -m main --task all \
  --master spark://spark-master:7077 \
  --players ../../data/lab4/players.csv \
  --salaries ../../data/lab4/salaries_1985to2018.csv \
  --seasons-stats ../../data/lab4/Seasons_Stats.csv \
  --processed-path ../../data/lab4/processed.parquet
```

