from __future__ import annotations

from pathlib import Path

from config.config import create_spark_session
from src.lab3.tf import WordTF, compute_top_term_frequencies


def write_text(tmp_path: Path, content: str) -> str:
    path = tmp_path / "sample.txt"
    path.write_text(content, encoding="utf-8")
    return str(path)


def test_compute_top_tf_filters_short_words(tmp_path):
    file_path = write_text(tmp_path, "cat cathedral cathedral dogmatic")

    spark = create_spark_session("local[1]", app_name="tf-test")
    try:
        results = compute_top_term_frequencies(
            spark,
            file_path,
            min_word_length=4,
            top_n=3,
        )
    finally:
        spark.stop()

    assert [record.word for record in results] == ["cathedral", "dogmatic"]
    assert results[0].count == 2


def test_compute_top_tf_returns_top_n(tmp_path):
    file_path = write_text(
        tmp_path,
        "apple banana banana apple banana pear pear pear",
    )

    spark = create_spark_session("local[1]", app_name="tf-top-n-test")
    try:
        records = compute_top_term_frequencies(
            spark,
            file_path,
            min_word_length=4,
            top_n=2,
        )
    finally:
        spark.stop()

    assert len(records) == 2
    assert [record.word for record in records] == ["banana", "apple"]
    assert isinstance(records[0], WordTF)
