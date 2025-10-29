from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


WORD_PATTERN = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")


@dataclass(frozen=True)
class WordTF:
    word: str
    count: int
    tf: float


RESULT_SCHEMA = StructType(
    [
        StructField("word", StringType(), False),
        StructField("count", LongType(), False),
        StructField("tf", DoubleType(), False),
    ]
)


def tokenize(line: str) -> Iterable[str]:
    """Extract normalized tokens from a line of text."""

    return WORD_PATTERN.findall(line.lower())


def compute_top_term_frequencies(
    spark: SparkSession,
    input_path: str,
    *,
    min_word_length: int = 4,
    top_n: int = 10,
) -> List[WordTF]:
    """Compute the top-N term frequencies for the provided text file."""

    if top_n <= 0:
        return []

    sc = spark.sparkContext
    lines = sc.textFile(input_path)

    words = (
        lines.flatMap(tokenize)
        .filter(lambda token: len(token) >= min_word_length)
    )

    words = words.persist(StorageLevel.MEMORY_ONLY)

    if words.isEmpty():
        words.unpersist()
        return []

    word_counts = words.map(lambda token: (token, 1)).reduceByKey(lambda a, b: a + b)
    word_counts = word_counts.persist(StorageLevel.MEMORY_ONLY)

    total_terms = word_counts.values().sum()
    if total_terms == 0:
        word_counts.unpersist()
        words.unpersist()
        return []

    top_records = word_counts.map(
        lambda pair: (pair[0], pair[1], pair[1] / total_terms)
    ).takeOrdered(
        top_n, key=lambda record: (-record[2], record[0])
    )

    word_counts.unpersist()
    words.unpersist()

    return [WordTF(word=word, count=int(count), tf=float(tf_value)) for word, count, tf_value in top_records]


def to_dataframe(spark: SparkSession, records: List[WordTF]) -> DataFrame:
    """Convert a list of ``WordTF`` records to a Spark DataFrame."""

    if not records:
        return spark.createDataFrame([], schema=RESULT_SCHEMA)

    rows = [(record.word, record.count, record.tf) for record in records]
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)