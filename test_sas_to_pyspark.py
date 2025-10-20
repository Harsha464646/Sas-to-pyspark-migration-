#!/usr/bin/env python3
"""
Unit tests for sas_to_pyspark.py

These tests validate key transformations on synthetic data using Spark-based assertions.
Run with: python test_sas_to_pyspark.py (requires PySpark installed)
Or in Databricks notebook: %run ./test_sas_to_pyspark
"""

import sys
import logging
from datetime import date

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ImportError:
    print("PySpark not available. Install with: pip install pyspark")
    sys.exit(1)

from sas_to_pyspark import (
    generate_synthetic_customers,
    generate_synthetic_transactions,
    clean_transactions,
    enrich_transactions,
    aggregate_monthly_region,
    compute_region_stats,
    flag_outliers,
)


def setup_test_spark() -> SparkSession:
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder.appName("test_sas_to_pyspark")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def test_generate_customers(spark: SparkSession) -> None:
    """Test customer generation."""
    print("Test: generate_synthetic_customers")
    df = generate_synthetic_customers(spark, seed=123, n_customers=10)
    assert df.count() == 10, "Expected 10 customers"
    cols = set(df.columns)
    assert {"CustomerID", "Name", "Region", "SignupDate"}.issubset(cols), "Missing columns"
    regions = {r[0] for r in df.select("Region").distinct().collect()}
    assert regions.issubset({"North", "South", "East", "West"}), f"Invalid regions: {regions}"
    ids = [r[0] for r in df.select("CustomerID").collect()]
    assert min(ids) == 1000 and max(ids) == 1009, "CustomerID range incorrect"
    print("  ✓ Passed")


def test_generate_transactions(spark: SparkSession) -> None:
    """Test transaction generation."""
    print("Test: generate_synthetic_transactions")
    df = generate_synthetic_transactions(spark, seed=456, n_transactions=100)
    assert df.count() == 100, "Expected 100 transactions"
    cols = set(df.columns)
    expected = {"TransactionID", "CustomerID", "TranDate", "Amount", "PaymentMethod", "Status"}
    assert expected.issubset(cols), f"Missing columns: {expected - cols}"
    methods = {r[0] for r in df.select("PaymentMethod").distinct().collect()}
    assert methods.issubset({"Card", "ACH", "Check", "Cash"}), f"Invalid payment methods: {methods}"
    statuses = {r[0] for r in df.select("Status").distinct().collect()}
    assert statuses.issubset({"Completed", "Refunded", ""}), f"Invalid statuses: {statuses}"
    print("  ✓ Passed")


def test_clean_transactions(spark: SparkSession) -> None:
    """Test transaction cleaning logic."""
    print("Test: clean_transactions")
    data = [
        (1, 1000, date(2024, 1, 15), 100.0, "Card", "Completed"),
        (2, 1001, date(2024, 2, 20), -50.0, "ACH", "Refunded"),
        (3, 1002, date(2024, 3, 10), 0.0, "Cash", ""),
        (4, 1003, date(2024, 4, 5), 200.0, "Check", None),
    ]
    schema = T.StructType([
        T.StructField("TransactionID", T.LongType(), False),
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("TranDate", T.DateType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("PaymentMethod", T.StringType(), True),
        T.StructField("Status", T.StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    cleaned = clean_transactions(df)

    status_map = {r[0]: r[1] for r in cleaned.select("TransactionID", "Status_clean").collect()}
    assert status_map[1] == "Completed", "Status_clean incorrect for Completed"
    assert status_map[2] == "Refunded", "Status_clean incorrect for Refunded"
    assert status_map[3] == "Unknown", "Status_clean incorrect for empty string"
    assert status_map[4] == "Unknown", "Status_clean incorrect for null"

    neg_map = {r[0]: r[1] for r in cleaned.select("TransactionID", "NegativeFlag").collect()}
    assert neg_map[1] == 0, "NegativeFlag should be 0 for positive amount"
    assert neg_map[2] == 1, "NegativeFlag should be 1 for negative amount"
    assert neg_map[3] == 0, "NegativeFlag should be 0 for zero amount"

    ym_map = {r[0]: r[1] for r in cleaned.select("TransactionID", "YearMonth").collect()}
    assert ym_map[1] == "202401", f"YearMonth incorrect: {ym_map[1]}"
    assert ym_map[2] == "202402", f"YearMonth incorrect: {ym_map[2]}"
    assert ym_map[3] == "202403", f"YearMonth incorrect: {ym_map[3]}"
    assert ym_map[4] == "202404", f"YearMonth incorrect: {ym_map[4]}"

    print("  ✓ Passed")


def test_enrich_transactions(spark: SparkSession) -> None:
    """Test enrichment with customer data."""
    print("Test: enrich_transactions")
    cust_data = [
        (1000, "Alice", "North", date(2023, 1, 1)),
        (1001, "Bob", "South", date(2023, 2, 1)),
    ]
    cust_schema = T.StructType([
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("Name", T.StringType(), False),
        T.StructField("Region", T.StringType(), False),
        T.StructField("SignupDate", T.DateType(), True),
    ])
    customers = spark.createDataFrame(cust_data, cust_schema)

    tx_data = [
        (1, 1000, date(2024, 1, 15), 100.0, "Card", "Completed", "Completed", 0, "202401"),
        (2, 1001, date(2024, 2, 20), 200.0, "ACH", "Completed", "Completed", 0, "202402"),
        (3, 9999, date(2024, 3, 10), 300.0, "Cash", "Completed", "Completed", 0, "202403"),
    ]
    tx_schema = T.StructType([
        T.StructField("TransactionID", T.LongType(), False),
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("TranDate", T.DateType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("PaymentMethod", T.StringType(), True),
        T.StructField("Status", T.StringType(), True),
        T.StructField("Status_clean", T.StringType(), True),
        T.StructField("NegativeFlag", T.IntegerType(), True),
        T.StructField("YearMonth", T.StringType(), True),
    ])
    tx_clean = spark.createDataFrame(tx_data, tx_schema)

    enriched = enrich_transactions(tx_clean, customers)

    rows = enriched.select("TransactionID", "Name", "Region").collect()
    row_map = {r[0]: (r[1], r[2]) for r in rows}
    assert row_map[1] == ("Alice", "North"), "Enrichment failed for customer 1000"
    assert row_map[2] == ("Bob", "South"), "Enrichment failed for customer 1001"
    assert row_map[3][0] == "Unknown_9999", "Unknown customer name incorrect"
    assert row_map[3][1] == "Unknown", "Unknown customer region incorrect"

    print("  ✓ Passed")


def test_aggregate_monthly_region(spark: SparkSession) -> None:
    """Test monthly aggregation."""
    print("Test: aggregate_monthly_region")
    data = [
        (1, 1000, date(2024, 1, 15), 100.0, "Card", "Completed", "Completed", 0, "202401", "Alice", "North", date(2023, 1, 1)),
        (2, 1001, date(2024, 1, 20), 200.0, "ACH", "Completed", "Completed", 0, "202401", "Bob", "North", date(2023, 2, 1)),
        (3, 1002, date(2024, 2, 10), -50.0, "Cash", "Refunded", "Refunded", 1, "202402", "Charlie", "South", date(2023, 3, 1)),
        (4, 1003, date(2024, 2, 15), 300.0, "Check", "Completed", "Completed", 0, "202402", "David", "South", date(2023, 4, 1)),
    ]
    schema = T.StructType([
        T.StructField("TransactionID", T.LongType(), False),
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("TranDate", T.DateType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("PaymentMethod", T.StringType(), True),
        T.StructField("Status", T.StringType(), True),
        T.StructField("Status_clean", T.StringType(), True),
        T.StructField("NegativeFlag", T.IntegerType(), True),
        T.StructField("YearMonth", T.StringType(), True),
        T.StructField("Name", T.StringType(), True),
        T.StructField("Region", T.StringType(), True),
        T.StructField("SignupDate", T.DateType(), True),
    ])
    tx_enriched = spark.createDataFrame(data, schema)

    monthly = aggregate_monthly_region(tx_enriched)

    rows = monthly.collect()
    assert len(rows) == 2, f"Expected 2 monthly records, got {len(rows)}"

    north_jan = [r for r in rows if r["Region"] == "North" and r["YearMonth"] == "202401"][0]
    assert north_jan["NumTrans"] == 2, "NumTrans incorrect"
    assert north_jan["TotalAmt"] == 300.0, "TotalAmt incorrect"
    assert north_jan["AvgAmt"] == 150.0, "AvgAmt incorrect"
    assert north_jan["NumNegatives"] == 0, "NumNegatives incorrect"

    south_feb = [r for r in rows if r["Region"] == "South" and r["YearMonth"] == "202402"][0]
    assert south_feb["NumTrans"] == 2, "NumTrans incorrect"
    assert south_feb["TotalAmt"] == 250.0, "TotalAmt incorrect"
    assert south_feb["AvgAmt"] == 125.0, "AvgAmt incorrect"
    assert south_feb["NumNegatives"] == 1, "NumNegatives incorrect"

    print("  ✓ Passed")


def test_outlier_detection(spark: SparkSession) -> None:
    """Test IQR outlier detection."""
    print("Test: compute_region_stats and flag_outliers")
    data = [
        (1, 1000, date(2024, 1, 1), 10.0, "Card", "Completed", "Completed", 0, "202401", "A", "North", date(2023, 1, 1)),
        (2, 1001, date(2024, 1, 2), 20.0, "Card", "Completed", "Completed", 0, "202401", "B", "North", date(2023, 1, 1)),
        (3, 1002, date(2024, 1, 3), 30.0, "Card", "Completed", "Completed", 0, "202401", "C", "North", date(2023, 1, 1)),
        (4, 1003, date(2024, 1, 4), 40.0, "Card", "Completed", "Completed", 0, "202401", "D", "North", date(2023, 1, 1)),
        (5, 1004, date(2024, 1, 5), 50.0, "Card", "Completed", "Completed", 0, "202401", "E", "North", date(2023, 1, 1)),
        (6, 1005, date(2024, 1, 6), 1000.0, "Card", "Completed", "Completed", 0, "202401", "F", "North", date(2023, 1, 1)),
    ]
    schema = T.StructType([
        T.StructField("TransactionID", T.LongType(), False),
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("TranDate", T.DateType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("PaymentMethod", T.StringType(), True),
        T.StructField("Status", T.StringType(), True),
        T.StructField("Status_clean", T.StringType(), True),
        T.StructField("NegativeFlag", T.IntegerType(), True),
        T.StructField("YearMonth", T.StringType(), True),
        T.StructField("Name", T.StringType(), True),
        T.StructField("Region", T.StringType(), True),
        T.StructField("SignupDate", T.DateType(), True),
    ])
    tx_enriched = spark.createDataFrame(data, schema)

    stats = compute_region_stats(tx_enriched, accuracy=10000)
    north_stats = stats.filter(F.col("Region") == "North").first()
    assert north_stats is not None, "North stats missing"
    assert abs(north_stats["Q1"] - 20.0) < 5, f"Q1 incorrect: {north_stats['Q1']}"
    assert abs(north_stats["Q3"] - 50.0) < 5, f"Q3 incorrect: {north_stats['Q3']}"
    assert abs(north_stats["IQR"] - 30.0) < 5, f"IQR incorrect: {north_stats['IQR']}"

    outliers = flag_outliers(tx_enriched, stats)
    flag_map = {r[0]: r[1] for r in outliers.select("TransactionID", "OutlierFlag").collect()}
    assert flag_map[6] == "High", "Transaction 6 should be High outlier"
    assert flag_map[1] in ["Normal", "Low"], "Transaction 1 should not be High outlier"
    assert flag_map[3] == "Normal", "Transaction 3 should be Normal"

    print("  ✓ Passed")


def test_end_to_end(spark: SparkSession) -> None:
    """Test end-to-end pipeline."""
    print("Test: end-to-end pipeline")
    customers = generate_synthetic_customers(spark, seed=999, n_customers=5)
    transactions = generate_synthetic_transactions(spark, seed=888, n_transactions=50)

    tx_clean = clean_transactions(transactions)
    tx_enriched = enrich_transactions(tx_clean, customers)
    monthly = aggregate_monthly_region(tx_enriched)
    stats = compute_region_stats(tx_enriched)
    outliers = flag_outliers(tx_enriched, stats)

    assert outliers.count() == transactions.count(), "Outlier count mismatch"
    assert monthly.count() > 0, "No monthly aggregates"
    assert stats.count() > 0, "No region stats"

    outlier_cols = set(outliers.columns)
    required = {"TransactionID", "CustomerID", "Amount", "Region", "YearMonth", "OutlierFlag", "Q1", "Q3", "IQR"}
    assert required.issubset(outlier_cols), f"Missing columns: {required - outlier_cols}"

    monthly_cols = set(monthly.columns)
    required_monthly = {"Region", "YearMonth", "NumTrans", "TotalAmt", "AvgAmt", "NumNegatives"}
    assert required_monthly.issubset(monthly_cols), f"Missing columns: {required_monthly - monthly_cols}"

    flags = {r[0] for r in outliers.select("OutlierFlag").distinct().collect()}
    assert flags.issubset({"High", "Low", "Normal"}), f"Invalid OutlierFlag values: {flags}"

    print("  ✓ Passed")


def run_all_tests() -> None:
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Running unit tests for sas_to_pyspark.py")
    print("=" * 60 + "\n")

    spark = setup_test_spark()
    spark.sparkContext.setLogLevel("WARN")

    tests = [
        test_generate_customers,
        test_generate_transactions,
        test_clean_transactions,
        test_enrich_transactions,
        test_aggregate_monthly_region,
        test_outlier_detection,
        test_end_to_end,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test(spark)
            passed += 1
        except AssertionError as e:
            print(f"  ✗ Failed: {e}")
            failed += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 60 + "\n")

    spark.stop()

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_all_tests()
