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
    return WORD_PATTERN.findall(line.lower())


def compute_top_term_frequencies(
    spark: SparkSession,
    input_path: str,
    *,
    min_word_length: int = 4,
    top_n: int = 10,
) -> List[WordTF]:

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

    def build_word_tf(item: tuple[str, int]) -> WordTF:
        word, count = item
        tf_value = count / total_terms
        return WordTF(word=word, count=int(count), tf=float(tf_value))

    top_records = word_counts.map(build_word_tf).takeOrdered(
        top_n, key=lambda record: (-record.tf, record.word)
    )

    word_counts.unpersist()
    words.unpersist()

    return top_records


def to_dataframe(spark: SparkSession, records: List[WordTF]) -> DataFrame:
    # Convert a list of WordTF records to DataFrame

    if not records:
        return spark.createDataFrame([], schema=RESULT_SCHEMA)

    rows = [(record.word, record.count, record.tf) for record in records]
    return spark.createDataFrame(rows, schema=RESULT_SCHEMA)