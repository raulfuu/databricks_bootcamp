# Databricks notebook source
import json
from pyspark.sql.functions import current_timestamp, expr, count, avg, round
from src.core.config_parser import load_config

try:
    import dlt
except ImportError:
    pass

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()


# Capture parameters passed from the DLT Pipeline settings
env_catalog = spark.conf.get("pipeline.env_catalog", "dev_bootcamp")
config_path = spark.conf.get("pipeline.config_path")

# Load and parse the metadata contract
config = load_config(config_path, env_catalog)

source_config = config["sources"][0]
transformations_config = config["transformations"]
source_path = source_config.get("path").replace("/*", "")

# METADATA COMPILER: Dynamically build DLT Expectations from JSON
val_config = next((t for t in transformations_config if t.get("type") == "validate_fields"), None)
dlt_expectations = {}
inverse_rules = []

if val_config:
    for rule in val_config.get("params", {}).get("validations", []):
        field = rule.get("field")
        for val_type in rule.get("validations", []):
            if val_type == "notNull":
                rule_name = f"{field}_not_null"
                sql_expr = f"{field} IS NOT NULL"
            elif val_type == "notEmpty":
                rule_name = f"{field}_not_empty"
                sql_expr = f"{field} IS NOT NULL AND {field} != ''"
            elif val_type == "isAdult":
                rule_name = f"{field}_must_be_adult"
                sql_expr = f"{field} >= 18"
            else:
                continue
            
            # Map for the native OK Table expectations decorator
            dlt_expectations[rule_name] = sql_expr
            # Map for capturing rejections in the KO table via inverse logic
            inverse_rules.append(f"NOT ({sql_expr})")

# Fallback
if not dlt_expectations:
    dlt_expectations["schema_handshake_valid"] = "1 == 1"

# Bronze Layer - Raw Streaming Ingestion
@dlt.table(
    name="person_raw_dlt",
    comment="Raw streaming ingestion from landing zone using Auto Loader"
)
def person_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(source_path)
        .withColumn("dt", current_timestamp())
    )


# Silver Layer - Native Expectations Quality Split

# STANDARD_OK Table (Enforces Quality Metrics)
@dlt.table(
    name="person_standard_ok_dlt",
    comment="Validated records that passed native DLT expectations metadata contract"
)
# Injects JSON rules straight into the Databricks Platform Telemetry Engine!
@dlt.expect_all_or_drop(dlt_expectations)
def person_standard_ok():
    return dlt.read("person_raw_dlt")


# STANDARD_KO Table (Captures Rejections via Inverse Filter and Error Code)
@dlt.table(
    name="person_standard_ko_dlt",
    comment="Rejected records that failed at least one metadata quality expectation"
)
def person_standard_ko():
    if not inverse_rules:
        return dlt.read("person_raw_dlt").filter("1 == 0")
        
    ko_filter_expression = " OR ".join(inverse_rules)
    ko_df = dlt.read("person_raw_dlt").filter(ko_filter_expression)
    
    # Dynamically build the arraycoderrorbyfield column using SQL expressions, evaluates every rule. If it fails, it adds the rule name. If it passes, it adds NULL.
    error_cases = []
    for rule_name, sql_expr in dlt_expectations.items():
        error_cases.append(f"CASE WHEN NOT ({sql_expr}) THEN '{rule_name}' ELSE NULL END")
        
    # We pack them into a SQL array and use Spark's high-order 'filter' function to remove the NULLs
    dynamic_array_sql = f"filter(array({', '.join(error_cases)}), x -> x IS NOT NULL)"
    
    # Attach the physical column to the final KO dataframe
    return ko_df.withColumn("arraycoderrorbyfield", expr(dynamic_array_sql))


# Gold Layer - Aggregated Metrics
@dlt.table(
    name="person_gold_office_stats_dlt",
    comment="Aggregated metrics by office for BI consumption (DLT Edition)"
)
def person_gold_office_stats():
    return (
        dlt.read("person_standard_ok_dlt")
        .groupBy("office")
        .agg(
            count("*").alias("total_employees"),
            round(avg("age"), 1).alias("average_age")
        )
    )