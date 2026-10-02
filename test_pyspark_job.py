import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

COLUMNS = ["order_id", "order_date", "name", "amount"]


@pytest.fixture(scope="module")
def spark_local():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-customer-orders")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_clean_data_keeps_valid_records(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", 150.0),
        ("1002", "2026-09-02", "Bob", 75.5),
    ]

    df = spark_local.createDataFrame(data, COLUMNS)

    results = clean_data(df).collect()

    assert len(results) == 2
    assert {r["name"] for r in results} == {"Alice", "Bob"}


def test_clean_data_removes_zero_and_negative_values(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", 150.0),
        ("1002", "2026-09-02", "Bob", -20.0),
        ("1003", "2026-09-03", "Sara", 0.0),
    ]

    df = spark_local.createDataFrame(data, COLUMNS)

    results = clean_data(df).collect()

    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["name"] == "Alice"
    assert results[0]["amount"] == 150.0


def test_clean_data_removes_null_names(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", 150.0),
        ("1002", "2026-09-02", None, 200.0),
    ]

    df = spark_local.createDataFrame(data, COLUMNS)

    results = clean_data(df).collect()

    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["name"] == "Alice"


def test_clean_data_calculates_amount_with_tax(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", 100.0),
        ("1002", "2026-09-02", "Bob", 50.0),
    ]

    df = spark_local.createDataFrame(data, COLUMNS)

    transformed_df = clean_data(df)
    results = {r["order_id"]: r["amount_with_tax"] for r in transformed_df.collect()}

    assert "amount_with_tax" in transformed_df.columns
    assert results["1001"] == pytest.approx(120.0)
    assert results["1002"] == pytest.approx(60.0)
