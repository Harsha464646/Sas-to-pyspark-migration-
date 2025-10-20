# SAS to PySpark Conversion: Complex Example

This repository contains a complete, production-quality PySpark conversion of a complex SAS program. The conversion maintains exact business logic, column names, formats, and outputs.

## Overview

The original SAS program:
- Generates sample customer and transaction data
- Cleans data and standardizes formats
- Uses hash tables for fast customer lookups
- Aggregates monthly metrics by region
- Detects outliers using IQR method per region
- Produces region-level reports
- Exports CSV outputs

The PySpark conversion replicates all functionality using distributed Spark operations suitable for large-scale data processing.

## Deliverables

1. **`sas_to_pyspark.py`** - Main PySpark script with all business logic
2. **`test_sas_to_pyspark.py`** - Unit tests with Spark-based assertions
3. **`README.md`** - This file with run instructions and migration notes
4. **Sample outputs** - `tx_outliers.csv` and `monthly_region.csv` for comparison

## Requirements

### Databricks Runtime
- **Recommended**: Databricks Runtime 13.3 LTS or higher
- **Minimum**: Databricks Runtime 11.3 LTS (Spark 3.3+)
- **Python**: 3.9+
- **PySpark**: 3.3+ (pre-installed in Databricks)

### Cluster Configuration
- **Driver**: 8GB+ memory
- **Workers**: 2+ workers with 16GB+ memory each
- **Autoscaling**: Recommended for production workloads

### Dependencies
No external dependencies required beyond standard Databricks/PySpark installation. The script uses only:
- `pyspark.sql` (DataFrame API)
- Python standard library (`argparse`, `logging`, `datetime`, `random`)

## Running in Databricks

### Option 1: Databricks Notebook

1. **Upload the script** to your Databricks workspace:
   ```
   /Workspace/Users/<your-email>/sas_to_pyspark.py
   ```

2. **Create a new Python notebook** and run:
   ```python
   # Import the main function
   %run ./sas_to_pyspark
   
   # Run with default settings (generates synthetic data)
   main_entry([
       "--output-base", "/dbfs/tmp/sas_outputs",
       "--single-file",
       "-vv"
   ])
   ```

3. **View outputs**:
   ```python
   # Read the outputs
   tx_outliers = spark.read.csv("/dbfs/tmp/sas_outputs/tx_outliers.csv", header=True, inferSchema=True)
   monthly_region = spark.read.csv("/dbfs/tmp/sas_outputs/monthly_region.csv", header=True, inferSchema=True)
   
   display(tx_outliers)
   display(monthly_region)
   ```

4. **Run unit tests**:
   ```python
   %run ./test_sas_to_pyspark
   ```

### Option 2: Databricks Job

1. **Upload the script** to DBFS:
   ```bash
   databricks fs cp sas_to_pyspark.py dbfs:/FileStore/scripts/sas_to_pyspark.py
   ```

2. **Create a job** via Databricks UI or CLI:
   ```json
   {
     "name": "SAS to PySpark Pipeline",
     "tasks": [
       {
         "task_key": "run_pipeline",
         "spark_python_task": {
           "python_file": "dbfs:/FileStore/scripts/sas_to_pyspark.py",
           "parameters": [
             "--output-base", "dbfs:/tmp/sas_outputs",
             "--single-file",
             "--write-parquet",
             "-vv"
           ]
         },
         "new_cluster": {
           "spark_version": "13.3.x-scala2.12",
           "node_type_id": "i3.xlarge",
           "num_workers": 2
         }
       }
     ]
   }
   ```

3. **Run the job** and monitor logs in the Databricks UI.

### Option 3: Using Real Data

To process your own customer and transaction data:

```python
main_entry([
    "--customers-path", "dbfs:/data/customers.parquet",
    "--transactions-path", "dbfs:/data/transactions.parquet",
    "--output-base", "dbfs:/output/sas_results",
    "--single-file",
    "--write-parquet",
    "-vv"
])
```

**Input data requirements**:
- **Customers**: Must have columns `CustomerID` (int), `Name` (string), `Region` (string), `SignupDate` (date)
- **Transactions**: Must have columns `TransactionID` (long), `CustomerID` (int), `TranDate` (date), `Amount` (double), `PaymentMethod` (string), `Status` (string)

## Running with spark-submit (Outside Databricks)

For local development or non-Databricks Spark clusters:

1. **Install PySpark**:
   ```bash
   pip install pyspark
   ```

2. **Run the script**:
   ```bash
   spark-submit \
     --master local[4] \
     --driver-memory 4g \
     sas_to_pyspark.py \
     --output-base /tmp/sas_outputs \
     --create-session \
     -vv
   ```

3. **Run unit tests**:
   ```bash
   python test_sas_to_pyspark.py
   ```

## Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--output-base` | Base output path (DBFS, S3, or local) | `/tmp/sas_outputs` |
| `--single-file` | Coalesce to single CSV file | `False` |
| `--write-parquet` | Also write Parquet copies | `False` |
| `--region-reports` | Regions to log reports for | `North South East West Unknown` |
| `--customers-path` | Input customers path (CSV/Parquet) | Generate synthetic |
| `--transactions-path` | Input transactions path (CSV/Parquet) | Generate synthetic |
| `--create-session` | Create SparkSession (for spark-submit only) | `False` |
| `-v, -vv` | Increase logging verbosity | `WARNING` |
| `--run-checks` | Run unit tests and exit | `False` |

## Output Files

### tx_outliers.csv
Contains all transactions with outlier flags and IQR statistics:
- All original transaction columns
- `Status_clean`: Standardized status (Completed, Refunded, Unknown)
- `NegativeFlag`: 1 if Amount < 0, else 0
- `YearMonth`: Format YYYYMM (e.g., "202401")
- `Name`, `Region`, `SignupDate`: Customer info
- `Q1`, `Q3`, `IQR`: Region-level quartiles and interquartile range
- `OutlierFlag`: "High", "Low", or "Normal"

### monthly_region.csv
Monthly aggregates by region:
- `Region`: Customer region
- `YearMonth`: Format YYYYMM
- `NumTrans`: Count of transactions
- `TotalAmt`: Sum of amounts
- `AvgAmt`: Average amount
- `NumNegatives`: Count of negative amounts

## Migration Notes

### Semantic Differences from SAS

#### 1. Percentile Calculation
- **SAS**: `PROC UNIVARIATE` uses exact percentiles (Definition 5 by default)
- **PySpark**: `percentile_approx()` uses approximate percentiles with configurable accuracy
- **Impact**: For large datasets, results may differ slightly (typically <1% for accuracy=10000)
- **Mitigation**: Increase accuracy parameter in `compute_region_stats()` for more precision (at cost of performance)

#### 2. Random Number Generation
- **SAS**: Uses `CALL STREAMINIT` with specific algorithms
- **PySpark**: Uses Python's `random` module with different algorithms
- **Impact**: Synthetic data will differ from SAS even with same seed
- **Mitigation**: For production, use real data inputs instead of synthetic generation

#### 3. Hash Lookup vs. Broadcast Join
- **SAS**: Uses hash object for in-memory lookup
- **PySpark**: Uses broadcast join (broadcasts smaller table to all workers)
- **Impact**: Functionally equivalent; PySpark scales better for large datasets
- **Recommendation**: Ensure customer table fits in driver memory (typically <2GB)

#### 4. Date Formatting
- **SAS**: `date9.` format produces "01JAN2024"
- **PySpark**: Uses ISO format "2024-01-01" internally
- **Impact**: YearMonth format is identical ("YYYYMM")
- **Note**: If you need SAS-style date display, add custom formatting in output step

#### 5. Missing Value Handling
- **SAS**: Distinguishes between missing (.) and empty string ('')
- **PySpark**: Uses `null` for both cases
- **Impact**: Status cleaning logic treats both as "Unknown" (consistent behavior)

#### 6. Sorting Stability
- **SAS**: Stable sort (preserves original order for ties)
- **PySpark**: Non-stable sort by default
- **Impact**: Ties in Amount may appear in different order
- **Mitigation**: Add secondary sort key if order matters

### Performance Tuning for Large Data

#### 1. Partitioning Strategy
```python
# Repartition by Region for better parallelism in aggregations
tx_enriched = tx_enriched.repartition("Region")
```

#### 2. Broadcast Join Threshold
```python
# Adjust broadcast threshold if customer table is large
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

#### 3. Shuffle Partitions
```python
# Increase for large datasets (default is 200)
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

#### 4. Caching Strategy
```python
# Cache intermediate results if reused multiple times
tx_enriched.cache()
tx_enriched.count()  # Materialize cache
```

#### 5. Coalesce for Output
```python
# For very large outputs, avoid single-file coalesce
# Instead, write partitioned and merge externally if needed
monthly_region.write.partitionBy("Region").parquet(output_path)
```

#### 6. Percentile Accuracy Trade-off
```python
# Lower accuracy for faster computation on huge datasets
region_stats = compute_region_stats(tx_enriched, accuracy=1000)
```

### Data Volume Recommendations

| Data Size | Cluster Config | Shuffle Partitions | Percentile Accuracy |
|-----------|----------------|-------------------|---------------------|
| < 1GB | 2 workers, 8GB | 50 | 10000 |
| 1-10GB | 4 workers, 16GB | 200 | 10000 |
| 10-100GB | 8 workers, 32GB | 400 | 5000 |
| 100GB-1TB | 16+ workers, 64GB+ | 800+ | 1000 |
| > 1TB | 32+ workers, 128GB+ | 1600+ | 1000 |

## Testing

### Run Unit Tests
```bash
# Local
python test_sas_to_pyspark.py

# Databricks notebook
%run ./test_sas_to_pyspark
```

### Run Built-in Checks
```bash
# With spark-submit
spark-submit sas_to_pyspark.py --run-checks --create-session

# In Databricks
main_entry(["--run-checks"])
```

### Validate Outputs
Compare the generated `tx_outliers.csv` and `monthly_region.csv` with the SAS-equivalent samples provided:
- Row counts should match
- Column names should match exactly
- YearMonth format should be YYYYMM
- OutlierFlag logic should be consistent (High/Low/Normal)
- Aggregations should be within rounding tolerance

## Troubleshooting

### Issue: "No active SparkSession"
**Solution**: In Databricks notebooks, the `spark` session is pre-provided. For spark-submit, add `--create-session` flag.

### Issue: "OutOfMemoryError" during broadcast join
**Solution**: Increase driver memory or disable broadcast join:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### Issue: Percentiles differ from SAS
**Solution**: Increase accuracy parameter or use exact percentiles (slower):
```python
# For exact percentiles (not recommended for large data)
df.approxQuantile("Amount", [0.25, 0.75], 0.0)
```

### Issue: Single-file CSV not created
**Solution**: This is a Databricks-specific feature. For local/spark-submit, manually merge part files or use the directory output.

### Issue: "Permission denied" writing to DBFS
**Solution**: Ensure you have write permissions to the output path. Try `/dbfs/tmp/` or your user directory.

## SAS Block Mapping

| SAS Block | PySpark Function | Notes |
|-----------|------------------|-------|
| `data customers;` | `generate_synthetic_customers()` | Driver-only for tiny data |
| `data transactions;` | `generate_synthetic_transactions()` | Driver-only for tiny data |
| `proc format;` | `clean_transactions()` | Status standardization |
| `data tx_clean;` | `clean_transactions()` | NegativeFlag, YearMonth |
| `data tx_enriched;` (hash) | `enrich_transactions()` | Broadcast join |
| `proc sql;` (aggregate) | `aggregate_monthly_region()` | GroupBy + agg |
| `proc univariate;` | `compute_region_stats()` | Percentile_approx |
| `proc sql;` (outliers) | `flag_outliers()` | IQR logic |
| `%macro region_report;` | `region_report()` | Logging only |
| `proc export;` | `write_csv()` | CSV with header |
| `libname outlib;` | `write_parquet()` | Optional Parquet |

## Additional Features

### Write to S3
```python
main_entry([
    "--output-base", "s3a://my-bucket/sas-outputs",
    "--write-parquet",
    "-vv"
])
```

### Custom Region Reports
```python
main_entry([
    "--region-reports", "North", "East",
    "-vv"
])
```

### Logging Levels
- No `-v`: WARNING only
- `-v`: INFO level (recommended)
- `-vv`: DEBUG level (verbose)

## Contact & Support

For questions or issues with this conversion:
1. Check the troubleshooting section above
2. Review the migration notes for semantic differences
3. Examine the unit tests for usage examples
4. Consult Databricks documentation for platform-specific issues

## License

This conversion is provided as-is for migration purposes. Adapt as needed for your production environment.
