# Docker Setup Update

## Running Lab 2

### Setup Instructions

1. Make the entrypoint script executable:
   ```powershell
   git update-index --chmod=+x docker-entrypoint.sh
   ```
   
   ```bash
   # On Linux/Mac
   chmod +x docker-entrypoint.sh
   ```

2. Start the Spark cluster:
   ```powershell
   docker compose up -d spark-master spark-worker
   ```

3. Run the tasks using the client container:

   **Download Task**
   ```powershell
   docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab2/main.py --task download --data-path /app/data/ecommerce.parquet
   ```

   **EDA Task**
   ```powershell
   docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab2/main.py --task eda --data-path /app/data/ecommerce.parquet
   ```

   **Analytics Task**
   ```powershell
   docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab2/main.py --task analytics --data-path /app/data/ecommerce.parquet --output-dir /app/results
   ```

   **All Tasks**
   ```powershell
   docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab2/main.py --task all --data-path /app/data/ecommerce.parquet --output-dir /app/results
   ```

4. Stop the cluster when finished:
   ```powershell
   docker compose down
   ```

## Running Lab 3

1. Ensure the Spark cluster is running:
   ```powershell
   docker compose up -d spark-master spark-worker
   ```

2. Launch the TF analysis job from the client container (saves results to `/app/results/lab3`):
   ```powershell
   docker compose run --rm client spark-submit --master spark://spark-master:7077 --deploy-mode client src/lab3/main.py --input /app/data/AllCombined.txt --top 10 --min-length 4 --output /app/results/lab3/top_tf_words --format csv
   ```

3. Stop the cluster when finished:
   ```powershell
   docker compose down
   ```

## Notes

- Kaggle configuration directory is set to `/tmp/kaggle_home`.
- Output files will be saved to the local file system.

If you encounter issues with kagglehub, provide Kaggle API credentials:

1. Create a directory for Kaggle credentials in container:
   ```powershell
   docker compose exec client mkdir -p /tmp/kaggle_home
   ```

2. Create a kaggle.json with credentials:
   ```powershell
   docker compose exec client bash -c 'echo "{\"username\":\"your-kaggle-username\",\"key\":\"your-kaggle-api-key\"}" > /tmp/kaggle_home/kaggle.json'
   ```

3. Set permissions:
   ```powershell
   docker compose exec client chmod 600 /tmp/kaggle_home/kaggle.json
   ```