import argparse
from pyspark.sql import SparkSession
from config.config import create_spark_session

# Самый быстрый и эффективный
# F(2k)=F(k)(2F(k+1)-F(k))
# F(2k+1)=F(k+1)**2+F(k)**2
# источник: https://km.mmf.bsu.by/courses/2021/vp2/fibonacci.pdf
def fib_fast_doubling(n: int) -> int:
    if n < 0:
        raise ValueError("N must be non-negative")

    def _fd(k: int) -> tuple[int, int]:
        if k == 0:
            return 0, 1
        a, b = _fd(k >> 1)
        c = a * (2 * b - a)
        d = a * a + b * b
        if k & 1:
            return d, c + d
        return c, d

    return _fd(n)[0]


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
