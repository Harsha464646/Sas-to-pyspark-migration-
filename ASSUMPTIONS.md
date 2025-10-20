# Assumptions and Design Decisions

This document lists all assumptions made during the SAS to PySpark conversion.

## Environment Assumptions

### Databricks Runtime
- **Target**: Databricks Runtime 13.3 LTS or higher
- **Minimum**: Databricks Runtime 11.3 LTS (Spark 3.3+)
- **Rationale**: Ensures access to modern PySpark APIs including `percentile_approx()` and improved broadcast join optimizations

### Cluster Configuration
- **Driver Memory**: 8GB minimum
  - Sufficient for broadcast joins with customer table (<2GB)
  - Handles driver-side synthetic data generation
- **Worker Memory**: 16GB per worker minimum
  - Supports shuffle operations for aggregations
  - Handles IQR calculations on partitioned data
- **Worker Count**: 2+ workers recommended
  - Enables parallel processing of regions
  - Scales linearly with data volume

### Memory Considerations
- Customer table assumed to fit in driver memory for broadcast join
- If customer table exceeds 2GB, consider regular join instead of broadcast
- Transaction data can be arbitrarily large (distributed processing)

## Data Assumptions

### Input Data Characteristics
- **Customers**: 
  - Small dimension table (thousands to millions of rows)
  - Fits in memory for broadcast join
  - No duplicate CustomerIDs
- **Transactions**:
  - Large fact table (millions to billions of rows)
  - May contain negative amounts (flagged but retained)
  - May contain missing/null Status values
  - CustomerID may reference non-existent customers (handled as "Unknown")

### Data Types
- **CustomerID**: Integer (32-bit sufficient for up to 2B customers)
- **TransactionID**: Long (64-bit for unlimited transactions)
- **Amount**: Double (sufficient precision for currency)
- **Dates**: Date type (not timestamp) - time component ignored
- **Strings**: UTF-8 encoded, no length limits

### Data Quality
- **Missing Values**: Handled gracefully
  - Missing Status → "Unknown"
  - Missing customer info → "Unknown_<CustomerID>"
- **Negative Amounts**: Flagged but retained (business decision)
- **Outliers**: Detected but not removed (business decision)

## Business Logic Assumptions

### Status Standardization
- Only "Completed" and "Refunded" are valid statuses
- All other values (including null, empty string) → "Unknown"
- Case-sensitive matching (as per SAS logic)

### YearMonth Format
- Format: YYYYMM (e.g., "202401" for January 2024)
- Lexicographic sorting matches chronological sorting
- No timezone considerations (uses date only)

### Outlier Detection
- **Method**: IQR (Interquartile Range) per region
- **Thresholds**:
  - High outlier: Amount > Q3 + 1.5 × IQR
  - Low outlier: Amount < Q1 - 1.5 × IQR
  - Normal: Everything else
- **Scope**: Per-region calculation (not global)
- **Retention**: All outliers are flagged but retained in output

### Region Handling
- Regions are case-insensitive for reporting ("North" = "NORTH")
- Unknown region is a valid category
- No validation of region values (any string accepted)

## Technical Assumptions

### Spark Session Management
- **Databricks Notebooks**: Pre-provided `spark` session exists
- **spark-submit**: User must pass `--create-session` flag
- **No re-creation**: Never create a new session if one exists

### Broadcast Join Strategy
- Customer table is small enough to broadcast (<2GB)
- Broadcast join is more efficient than shuffle join for this use case
- If customer table grows large, automatic fallback to shuffle join

### Percentile Calculation
- **Method**: `percentile_approx()` with accuracy=10000
- **Trade-off**: Speed vs. precision
  - Accuracy 10000: ~0.01% error, suitable for most use cases
  - Exact percentiles available but slow for large data
- **Difference from SAS**: May differ slightly due to approximation algorithm

### Partitioning
- Default Spark partitioning used (200 shuffle partitions)
- No explicit repartitioning unless data is skewed
- Users can tune `spark.sql.shuffle.partitions` for their data size

### Output Format
- **CSV**: Human-readable, compatible with SAS outputs
- **Parquet**: Optional, for downstream Spark processing
- **Single-file**: Optional coalesce for small outputs (not recommended for large data)

## Synthetic Data Generation

### Purpose
- Demonstration and testing only
- Not intended for production use
- Matches SAS logic but uses different RNG algorithms

### Characteristics
- **Customers**: 20 by default, evenly distributed across regions
- **Transactions**: 600 by default, normal distribution for amounts
- **Outliers**: ~1% extreme outliers (10x normal amount)
- **Negative Amounts**: ~5-10% of transactions
- **Missing Status**: ~2% of transactions

### Limitations
- Generated on driver (not distributed)
- Only suitable for small datasets (<10K rows)
- For production, use real data inputs

## Performance Assumptions

### Data Volume Targets
- **Small**: <1GB - single node sufficient
- **Medium**: 1-100GB - 2-8 workers recommended
- **Large**: 100GB-1TB - 8-16 workers recommended
- **Very Large**: >1TB - 16+ workers, tune shuffle partitions

### Bottlenecks
- **Broadcast Join**: Limited by driver memory
- **Percentile Calculation**: Requires full shuffle, can be slow
- **Outlier Flagging**: Requires join with stats, moderate cost
- **CSV Output**: Single-file coalesce is expensive for large data

### Optimization Opportunities
- Cache `tx_enriched` if reused multiple times
- Repartition by Region before aggregations if data is skewed
- Lower percentile accuracy for faster computation on huge datasets
- Write partitioned Parquet instead of single CSV for large outputs

## Compatibility Assumptions

### SAS Compatibility
- **Logic**: 100% compatible with SAS business logic
- **Outputs**: Column names and formats match exactly
- **Semantics**: Minor differences in percentile calculation (documented)
- **Random Data**: Different RNG algorithms, outputs will differ

### Databricks Compatibility
- Uses standard PySpark APIs (no Databricks-specific features required)
- Optional: Can use `dbutils` for single-file CSV rename
- Works on Databricks Community Edition (with smaller data)

### Spark Version Compatibility
- **Minimum**: Spark 3.3 (for modern DataFrame APIs)
- **Recommended**: Spark 3.4+ (for performance improvements)
- **Tested**: Spark 3.5 (Databricks Runtime 13.3 LTS)

## Security Assumptions

### Data Access
- User has read access to input paths
- User has write access to output paths
- No authentication required for local/DBFS paths
- S3 access requires proper IAM roles/credentials

### Data Privacy
- No PII masking or encryption applied
- Outputs contain all input data (including sensitive fields)
- User responsible for applying data governance policies

### Logging
- Logs may contain sample data values
- Set verbosity to WARNING in production to minimize log exposure
- No secrets or credentials logged

## Testing Assumptions

### Unit Tests
- Run on local Spark (master="local[2]")
- Use small synthetic datasets (5-100 rows)
- Test core transformations, not performance
- Assume PySpark is installed and available

### Validation
- Sample outputs provided for comparison
- Row counts and column names should match
- Aggregations should be within rounding tolerance (±0.01)
- Outlier flags should be consistent (logic-based, not data-based)

## Limitations and Known Issues

### Not Implemented
- **SAS Macros**: Region reports are logged, not rendered as HTML/charts
- **PROC REPORT**: Cross-tab wide format not persisted (can be added)
- **ODS Graphics**: No chart generation (use Databricks visualization)
- **SAS Formats**: Custom formats not replicated (only Status format)

### Workarounds Required
- **Single-file CSV**: Requires manual merge of part files outside Databricks
- **Date Formats**: Use ISO format instead of SAS date9. format
- **Sorting Stability**: Add secondary sort key if order matters

### Future Enhancements
- Add support for custom input schemas
- Implement exact percentiles as an option
- Add data quality checks and validation
- Support incremental processing (append mode)
- Add support for multiple output formats (JSON, Avro, etc.)

## Deployment Assumptions

### Databricks Job
- Job uses cluster configuration from job definition
- No interactive debugging (use notebooks for development)
- Logs available in Databricks UI
- Outputs written to DBFS or S3

### spark-submit
- Spark installed and configured on local machine
- Sufficient local disk space for shuffle operations
- Network access for S3 writes (if applicable)
- User manages Spark configuration (memory, cores, etc.)

## Maintenance Assumptions

### Code Updates
- Single file design for easy deployment
- No external dependencies beyond PySpark
- Backward compatible with Spark 3.3+
- Well-commented for future modifications

### Monitoring
- Use Spark UI for performance monitoring
- Check logs for errors and warnings
- Validate output row counts and aggregations
- Monitor cluster resource utilization

## Documentation Assumptions

### User Expertise
- Basic understanding of PySpark and Databricks
- Familiarity with SAS concepts (for comparison)
- Ability to read Python code
- Access to Databricks documentation

### Support
- README provides comprehensive run instructions
- Migration notes explain semantic differences
- Unit tests demonstrate usage patterns
- Troubleshooting section covers common issues
