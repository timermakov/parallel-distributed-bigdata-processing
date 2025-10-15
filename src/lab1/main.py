import argparse
from pyspark.sql import SparkSession
from config.config import create_spark_session
from lab1.fibonacci import fib_fast_doubling


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute Nth Fibonacci using PySpark (client mode)")
    parser.add_argument("--n", type=int, required=True, help="Fibonacci index N (>=0)")
    parser.add_argument("--master", type=str, default=None, help="Spark master URL, e.g., local[*] or spark://spark-master:7077")
    args = parser.parse_args()

    spark: SparkSession = create_spark_session(args.master)
    try:
        result = fib_fast_doubling(args.n)
        print(f"Fibonacci({args.n}) = {result}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
