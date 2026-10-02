import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data


COLUMNS = ["order_id", "order_Date", "customer_name", "value"]
 
 
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
        ("1001", "2026-09-01", "Alice", "150.00"),
        ("1002", "2026-09-02", "Bob", "75.50"),
    ]
 
    df = spark_local.createDataFrame(data, COLUMNS)
 
    results = clean_data(df).collect()
 
    assert len(results) == 2
    assert {r["customer_name"] for r in results} == {"Alice", "Bob"}
 
 
def test_clean_data_removes_zero_and_negative_values(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", "150.00"),
        ("1002", "2026-09-02", "Bob", "-20.00"),
        ("1003", "2026-09-03", "Sara", "0"),
    ]
 
    df = spark_local.createDataFrame(data, COLUMNS)
 
    transformed_df = clean_data(df)
 
    results = transformed_df.collect()
 
    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["customer_name"] == "Alice"
    assert results[0]["value"] == 150.0
 
 
def test_clean_data_removes_null_customer_names(spark_local):
    data = [
        ("1001", "2026-09-01", "Alice", "150.00"),
        ("1002", "2026-09-02", None, "200.00"),
    ]
 
    df = spark_local.createDataFrame(data, COLUMNS)
 
    results = clean_data(df).collect()
 
    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["customer_name"] == "Alice"
 
 
def test_clean_data_calculates_value_with_tax(spark_local):
    
    data = [
        ("1001", "2026-09-01", "Alice", "100.00"),
        ("1002", "2026-09-02", "Bob", "50.00"),
    ]
 
    df = spark_local.createDataFrame(data, COLUMNS)
    transformed_df = clean_data(df)
    results = {r["order_id"]: r["value_with_tax"] for r in transformed_df.collect()}
    assert "value_with_tax" in transformed_df.columns
    assert results["1001"] == pytest.approx(120.0)
    assert results["1002"] == pytest.approx(60.0)
