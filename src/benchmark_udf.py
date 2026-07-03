# Databricks notebook source
import sys
import os
import time
from pyspark.sql.functions import udf, col, upper, regexp_replace
from pyspark.sql.types import StringType

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = os.getcwd()

project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.core.reader import get_spark_session


def run_performance_showdown():
    spark = get_spark_session()
    
    # Load the production dataset
    prod_path = "/Volumes/prod_bootcamp/data_ingestion/landing_zone/input_performance/events/person"
    print(f"Loading production dataset from: {prod_path}")
    
    # Databricks Serverless handle
    raw_df = spark.read.json(prod_path)
    total_rows = raw_df.count()
    print(f"Dataset successfully cached. Processing {total_rows:,} rows...")


    # 1. Python UDFs
    def legacy_clean_string(text):
        if text is None:
            return None
        # String standardization logic executed row-by-row in an isolated Python process
        cleaned = text.strip().upper()
        return f"EMP_{cleaned}"

    # Register standard UDF
    legacy_udf = udf(legacy_clean_string, StringType())
    
    print("\n[1/2] Starting Baseline Performance Test: Legacy Row-by-Row UDF...")
    start_udf = time.time()
    
    # Use 'noop' (No Operation) format to isolate compute performance from disk-writing bottlenecks
    raw_df.withColumn("processed_name", legacy_udf(col("name"))) \
          .write.mode("overwrite").format("noop").save()
          
    duration_udf = time.time() - start_udf
    print(f"--> Legacy UDF Execution Finished: {duration_udf:.2f} seconds")


    # 2. Spark Engine
    print("\n[2/2] Starting Optimized Performance Test: Native PySpark Expressions...")
    start_native = time.time()
    
    # Built-in functions execute directly inside the JVM/Tungsten engine with zero serialization overhead
    raw_df.withColumn("processed_name", regexp_replace(upper(col("name")), r'^\s+|\s+$', '')) \
          .withColumn("processed_name", regexp_replace(col("processed_name"), "^", "EMP_")) \
          .write.mode("overwrite").format("noop").save()
          
    duration_native = time.time() - start_native
    print(f"--> Native Spark Functions Finished: {duration_native:.2f} seconds")


    # Evaluation
    speedup_multiplier = duration_udf / duration_native
    print("\n=======================================================")
    print("                 BENCHMARK PERFORMANCE SUMMARY          ")
    print("=======================================================")
    print(f" Total Rows Evaluated:   {total_rows:,}")
    print(f" Legacy Python UDF:       {duration_udf:.2f} seconds")
    print(f" Native JVM Expressions:  {duration_native:.2f} seconds")
    print(f" PERFORMANCE IMPROVEMENT: {speedup_multiplier:.1f}x FASTER")
    print("=======================================================")

if __name__ == "__main__":
    run_performance_showdown()