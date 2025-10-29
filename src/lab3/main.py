from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import create_spark_session
from src.lab3.tf import WordTF, compute_top_term_frequencies, to_dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute top-N term frequencies for a text document using Spark"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/AllCombined.txt",
        help="Path to the input text file",
    )
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL, e.g., local[*] or spark://spark-master:7077",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of words with the highest TF values to output",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=4,
        help="Minimum word length to consider for TF calculation",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional output directory to store results (supports CSV or Parquet)",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="csv",
        choices=["csv", "parquet"],
        help="Storage format when --output is provided",
    )
    return parser.parse_args()


def write_results(df, output_path: str, fmt: str) -> None:
    path = Path(output_path)
    if fmt == "csv":
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(path))
        )
    else:
        df.coalesce(1).write.mode("overwrite").parquet(str(path))


def print_results(records: list[WordTF]) -> None:
    if not records:
        print("No tokens matched the specified constraints.")
        return

    print("Top words by term frequency:")
    for index, record in enumerate(records, start=1):
        print(f"{index:>2}. {record.word:<20} count={record.count:<6} tf={record.tf:.6f}")


def main() -> None:
    args = parse_args()

    spark: SparkSession = create_spark_session(args.master, app_name="WordTFApp")
    try:
        records = compute_top_term_frequencies(
            spark,
            args.input,
            min_word_length=args.min_length,
            top_n=args.top,
        )

        print_results(records)

        if args.output and records:
            df = to_dataframe(spark, records)
            ordered_df = df.orderBy(F.desc("tf"), F.asc("word"))
            write_results(ordered_df, args.output, args.format)
            print(f"Results written to {args.output} as {args.format}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()