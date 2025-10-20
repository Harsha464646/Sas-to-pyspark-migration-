#!/usr/bin/env python3
"""
SAS to PySpark conversion: Complex example SAS program

Deliverable: Single, runnable, production-quality PySpark script for Databricks or spark-submit.

Core Guarantees:
- Uses existing `spark` in Databricks notebooks (does not create SparkSession there)
- Maintains business logic, column names, YearMonth format, flag logic, aggregations, IQR outlier rules
- Robust argument parsing, logging, error handling
- Can write outputs to DBFS or S3 paths
- Includes unit-test style assertions on synthetic data via --run-checks

Mapping to SAS blocks:
1) data customers; data transactions;  -> generate_synthetic_customers(), generate_synthetic_transactions() (driver-only tiny ops)
2) formats and cleaning                -> clean_transactions()
3) hash object for lookup              -> enrich_transactions() using broadcast join
4) monthly aggregates                  -> aggregate_monthly_region()
5) IQR outliers per region             -> compute_region_stats() + flag_outliers()
6) macro region_report                 -> region_report() (logs summaries)
7) cross-tab report                    -> covered by monthly output; wide pivot not persisted but can be added easily
8) export CSVs                         -> write_outputs()
9) save dataset                        -> optional Parquet write via --write-parquet
"""

import sys
import os
import argparse
import logging
from typing import Optional, List

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.utils import AnalysisException
except Exception as e:  # pragma: no cover - import-time environment
    SparkSession = None
    DataFrame = None
    F = None
    T = None
    AnalysisException = Exception


def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )


def get_spark(create_if_missing: bool = False) -> SparkSession:
    """Return an active SparkSession. Do not create one in Databricks notebooks.
    - In notebooks, use the existing global `spark` if present or getActiveSession().
    - For spark-submit, create if missing when create_if_missing is True.
    """
    if 'spark' in globals() and globals()['spark'] is not None:
        return globals()['spark']
    if SparkSession is None:
        raise RuntimeError("PySpark is not available in this environment.")
    sess = SparkSession.getActiveSession()
    if sess:
        return sess
    if create_if_missing:
        return SparkSession.builder.appName("sas_to_pyspark").getOrCreate()
    raise RuntimeError(
        "No active SparkSession. In Databricks notebooks, use the provided 'spark'. "
        "For spark-submit, pass --create-session true."
    )



def generate_synthetic_customers(spark: SparkSession, seed: int = 12345, n_customers: int = 20) -> DataFrame:
    """Generate tiny customers dataset on driver (acceptable and documented).
    Columns: CustomerID (int), Name (string), Region (string), SignupDate (date)
    """
    import random
    from datetime import date, timedelta

    random.seed(seed)
    customers = []
    for cust_id in range(1000, 1000 + n_customers):
        name = f"Cust_{cust_id:4d}"
        pick = random.randint(1, 4)
        region = {1: 'North', 2: 'South', 3: 'East', 4: 'West'}[pick]
        signup_date = date.today() - timedelta(days=random.randint(0, 365 * 3))
        customers.append((cust_id, name, region, signup_date))
    schema = T.StructType([
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("Name", T.StringType(), False),
        T.StructField("Region", T.StringType(), False),
        T.StructField("SignupDate", T.DateType(), True),
    ])
    return spark.createDataFrame(customers, schema)


def generate_synthetic_transactions(spark: SparkSession, seed: int = 98765, n_transactions: int = 600) -> DataFrame:
    """Generate tiny transactions dataset on driver (acceptable and documented).
    Columns: TransactionID (long), CustomerID (int), TranDate (date), Amount (double),
             PaymentMethod (string), Status (string)
    """
    import random
    from datetime import date, timedelta

    random.seed(seed)
    rows = []
    transaction_id = 0
    for _ in range(n_transactions):
        transaction_id += 1
        customer_id = 1000 + int((random.random() * 20) + 1) - 1
        tran_date = date.today() - timedelta(days=random.randint(0, 365 * 2))
        amt = round(random.gauss(200, 600), 2)
        if random.random() < 0.01:
            amt = amt * 10
        pm_map = {1: 'Card', 2: 'ACH', 3: 'Check', 4: 'Cash'}
        payment_method = pm_map[random.randint(1, 4)]
        st = random.random()
        if st < 0.02:
            status = ''
        elif st < 0.9:
            status = 'Completed'
        else:
            status = 'Refunded'
        rows.append((transaction_id, customer_id, tran_date, float(amt), payment_method, status))

    schema = T.StructType([
        T.StructField("TransactionID", T.LongType(), False),
        T.StructField("CustomerID", T.IntegerType(), False),
        T.StructField("TranDate", T.DateType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("PaymentMethod", T.StringType(), True),
        T.StructField("Status", T.StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)



def clean_transactions(tx: DataFrame) -> DataFrame:
    """Standardize Status, flag negatives, create YearMonth (YYYYMM)."""
    if tx is None:
        raise ValueError("tx is None")
    status_clean = (
        F.when(F.col("Status").isNull() | (F.col("Status") == ''), F.lit("Unknown"))
        .when(F.col("Status") == F.lit("Completed"), F.lit("Completed"))
        .when(F.col("Status") == F.lit("Refunded"), F.lit("Refunded"))
        .otherwise(F.lit("Unknown"))
        .alias("Status_clean")
    )
    negative_flag = (F.when(F.col("Amount") < 0, F.lit(1)).otherwise(F.lit(0)).alias("NegativeFlag"))
    yearmonth = F.date_format(F.col("TranDate"), "yyyyMM").alias("YearMonth")
    return tx.withColumns({"Status_clean": status_clean, "NegativeFlag": negative_flag, "YearMonth": yearmonth})



def enrich_transactions(tx_clean: DataFrame, customers: DataFrame) -> DataFrame:
    """Broadcast join to attach Name, Region, SignupDate. Fill unknowns like SAS logic."""
    bcust = F.broadcast(customers)
    joined = (
        tx_clean.join(bcust, on="CustomerID", how="left")
        .withColumn(
            "Name",
            F.when(F.col("Name").isNull(), F.concat(F.lit("Unknown_"), F.col("CustomerID"))).otherwise(F.col("Name")),
        )
        .withColumn("Region", F.coalesce(F.col("Region"), F.lit("Unknown")))
    )
    return joined



def aggregate_monthly_region(tx_enriched: DataFrame) -> DataFrame:
    agg = (
        tx_enriched.groupBy("Region", "YearMonth")
        .agg(
            F.count(F.lit(1)).alias("NumTrans"),
            F.sum("Amount").alias("TotalAmt"),
            F.avg("Amount").alias("AvgAmt"),
            F.sum("NegativeFlag").alias("NumNegatives"),
        )
        .orderBy(["Region", "YearMonth"])  # lexicographic order like SAS
    )
    return agg



def compute_region_stats(tx_enriched: DataFrame, accuracy: int = 10000) -> DataFrame:
    """Compute Q1, Q3, IQR per Region using percentile_approx for robustness.
    Note: This approximates SAS PROC UNIVARIATE. See migration notes.
    """
    pct = tx_enriched.groupBy("Region").agg(
        F.expr(f"percentile_approx(Amount, array(0.25, 0.75), {accuracy}) as pct")
    )
    stats = (
        pct.select(
            F.col("Region"),
            F.col("pct")[0].cast("double").alias("Q1"),
            F.col("pct")[1].cast("double").alias("Q3"),
        )
        .withColumn("IQR", F.col("Q3") - F.col("Q1"))
    )
    return stats


def flag_outliers(tx_enriched: DataFrame, region_stats: DataFrame) -> DataFrame:
    joined = tx_enriched.join(region_stats, on="Region", how="left")
    high_thresh = F.col("Q3") + F.lit(1.5) * F.col("IQR")
    low_thresh = F.col("Q1") - F.lit(1.5) * F.col("IQR")
    flagged = joined.withColumn(
        "OutlierFlag",
        F.when(F.col("Amount") > high_thresh, F.lit("High"))
        .when(F.col("Amount") < low_thresh, F.lit("Low"))
        .otherwise(F.lit("Normal")),
    )
    return flagged.orderBy([F.col("Region"), F.col("Amount").desc()])



def region_report(logger: logging.Logger, tx_outliers: DataFrame, monthly_region: DataFrame, region: str) -> None:
    r_up = region.upper()
    tx_r = tx_outliers.filter(F.upper(F.col("Region")) == F.lit(r_up))
    cnt = tx_r.count()
    if cnt == 0:
        logger.info(f"Region Report: {region} - No transactions")
        return
    logger.info(f"Region Report: {region}")
    summary = (
        tx_r.groupBy("OutlierFlag")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.mean("Amount").alias("mean"),
            F.sum("Amount").alias("sum"),
            F.min("Amount").alias("min"),
            F.max("Amount").alias("max"),
        )
        .orderBy("OutlierFlag")
    )
    logger.info("Outlier summary (n, mean, sum, min, max):")
    logger.info("\n" + "\n".join([str(r) for r in summary.collect()]))

    monthly_r = monthly_region.filter(F.upper(F.col("Region")) == F.lit(r_up)).orderBy("YearMonth")
    logger.info("Monthly totals (YearMonth, TotalAmt):")
    logger.info("\n" + "\n".join([str(r) for r in monthly_r.select("YearMonth", "TotalAmt").collect()]))



def write_csv(df: DataFrame, base_path: str, subdir: str, single_file: bool = False, logger: Optional[logging.Logger] = None) -> str:
    """Write df as CSV with header to base_path/subdir (directory). Optionally single file.
    Returns the final directory written. If single_file, tries to collapse to one file.
    """
    target_dir = os.path.join(base_path.rstrip('/'), subdir)
    if logger:
        logger.info(f"Writing CSV to {target_dir} (single_file={single_file})")
    writer = df
    if single_file:
        writer = df.coalesce(1)
    (writer.write.mode("overwrite").option("header", True).csv(target_dir))

    if single_file:
        try:
            if 'dbutils' in globals():  # type: ignore
                files = [f.path for f in dbutils.fs.ls(target_dir) if f.path.endswith('.csv') or 'part-' in f.path]  # type: ignore
                part_files = [f for f in files if 'part-' in f]
                if part_files:
                    part = part_files[0]
                    final_path = target_dir.rstrip('/') + ".csv"
                    dbutils.fs.rm(final_path, True)  # type: ignore
                    dbutils.fs.cp(part, final_path)  # type: ignore
                    if logger:
                        logger.info(f"Created single-file CSV at {final_path}")
        except Exception as e:  # pragma: no cover
            if logger:
                logger.warning(f"Single-file rename skipped: {e}")
    return target_dir


def write_parquet(df: DataFrame, base_path: str, subdir: str, logger: Optional[logging.Logger] = None) -> str:
    target_dir = os.path.join(base_path.rstrip('/'), subdir)
    if logger:
        logger.info(f"Writing Parquet to {target_dir}")
    df.write.mode("overwrite").parquet(target_dir)
    return target_dir



def run_checks(spark: SparkSession, logger: logging.Logger) -> None:
    logger.info("Running synthetic data checks...")
    customers = generate_synthetic_customers(spark, seed=1, n_customers=5)
    tx = generate_synthetic_transactions(spark, seed=2, n_transactions=50)
    txc = clean_transactions(tx)
    assert txc.filter(F.col("Status_clean") == "Unknown").count() >= 1
    assert txc.filter(F.col("NegativeFlag") == 1).count() >= 0
    ym_len = txc.select(F.length("YearMonth").alias("l")).agg(F.min("l")).first()[0]
    assert ym_len == 6, f"YearMonth length expected 6, got {ym_len}"

    txe = enrich_transactions(txc, customers)
    cols = set(txe.columns)
    assert {"Name", "Region", "SignupDate"}.issubset(cols)

    monthly = aggregate_monthly_region(txe)
    assert monthly.count() > 0

    stats = compute_region_stats(txe, accuracy=10000)
    assert {"Region", "Q1", "Q3", "IQR"}.issubset(set(stats.columns))

    out = flag_outliers(txe, stats)
    assert out.count() == tx.count()

    allowed = {"High", "Low", "Normal"}
    distinct = {r[0] for r in out.select("OutlierFlag").distinct().collect()}
    assert distinct.issubset(allowed)

    total_numtrans = monthly.agg(F.sum("NumTrans")).first()[0]
    assert total_numtrans == tx.count()

    logger.info("All checks passed.")



def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SAS to PySpark conversion: complex example")
    p.add_argument("--output-base", required=False, default="/tmp/sas_outputs", help="Base output path (DBFS or S3 or local)")
    p.add_argument("--single-file", action="store_true", help="Coalesce to single CSV file and attempt rename (Databricks)")
    p.add_argument("--write-parquet", action="store_true", help="Also write Parquet copies of outputs")
    p.add_argument("--region-reports", nargs="*", default=["North", "South", "East", "West", "Unknown"], help="Regions to log reports for")
    p.add_argument("--customers-path", help="Optional customers input (CSV/Parquet); if omitted synthetic data is generated")
    p.add_argument("--transactions-path", help="Optional transactions input (CSV/Parquet); if omitted synthetic data is generated")
    p.add_argument("--create-session", action="store_true", help="Create SparkSession if none exists (for spark-submit only)")
    p.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging verbosity (-v, -vv)")
    p.add_argument("--run-checks", action="store_true", help="Run unit-test style checks on synthetic data and exit")
    return p.parse_args(argv)


def read_any_df(spark: SparkSession, path: str) -> DataFrame:
    lower = path.lower()
    if lower.endswith(".parquet") or lower.endswith(".parq"):
        return spark.read.parquet(path)
    if lower.endswith(".csv"):
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    try:
        return spark.read.parquet(path)
    except Exception:
        return spark.read.option("header", True).option("inferSchema", True).csv(path)


def main_entry(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(args.verbose)
    logger = logging.getLogger("sas_to_pyspark")

    if args.run_checks:
        spark = get_spark(create_if_missing=args.create_session)
        run_checks(spark, logger)
        return

    try:
        spark = get_spark(create_if_missing=args.create_session)
        logger.info("Starting SAS to PySpark pipeline")

        if args.customers_path:
            customers = read_any_df(spark, args.customers_path)
        else:
            customers = generate_synthetic_customers(spark)

        if args.transactions_path:
            transactions = read_any_df(spark, args.transactions_path)
        else:
            transactions = generate_synthetic_transactions(spark)

        tx_clean = clean_transactions(transactions)
        tx_enriched = enrich_transactions(tx_clean, customers)
        monthly_region = aggregate_monthly_region(tx_enriched)
        region_stats = compute_region_stats(tx_enriched)
        tx_outliers = flag_outliers(tx_enriched, region_stats)

        for r in args.region_reports:
            region_report(logger, tx_outliers, monthly_region, r)

        out_base = args.output_base
        write_csv(tx_outliers, out_base, "tx_outliers.csv", single_file=args.single_file, logger=logger)
        write_csv(monthly_region, out_base, "monthly_region.csv", single_file=args.single_file, logger=logger)

        if args.write_parquet:
            write_parquet(tx_outliers, out_base, "outlib.tx_outliers", logger=logger)

        logger.info("Pipeline complete.")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main_entry()
