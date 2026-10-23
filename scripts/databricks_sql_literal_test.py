"""Escaping for SQL literals built by the daily Databricks load."""

import importlib.util
from pathlib import Path

MODULE = Path(__file__).with_name("databricks_daily_finished.py")
spec = importlib.util.spec_from_file_location("databricks_daily_finished", MODULE)
daily = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(daily)


def test_spark_escapes_backslash_before_closing_quote():
    daily.SQL_DIALECT = "spark"
    assert daily.lit("one\\") == "'one\\\\'"
    assert daily.lit("doesn't\\") == "'doesn''t\\\\'"
    assert daily.lit("a\\'b") == "'a\\\\''b'"
    assert daily.lit("plain") == "'plain'"
    assert daily.lit(None) == "NULL"
    assert daily.lit(True) == "TRUE"
    assert daily.lit(4) == "4"


def test_postgres_keeps_literal_backslashes():
    daily.SQL_DIALECT = "postgres"
    try:
        assert daily.lit("one\\") == "'one\\'"
        assert daily.lit("don't") == "'don''t'"
    finally:
        daily.SQL_DIALECT = "spark"


if __name__ == "__main__":
    test_spark_escapes_backslash_before_closing_quote()
    test_postgres_keeps_literal_backslashes()
    print("ok")
