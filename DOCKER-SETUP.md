# Docker Setup Update

## Running Lab 2 with Docker Compose

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