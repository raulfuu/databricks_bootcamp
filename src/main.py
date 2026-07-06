# Databricks notebook source
import sys
import os
import time
from datetime import datetime
from pyspark.sql.functions import count, avg, round as spark_round

try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    pass

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = os.getcwd()

project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.logger import get_logger
from src.core.config_parser import load_config
from src.core.reader import get_spark_session, read_source_data
from src.core.validator import apply_validations
from src.core.transformer import add_metadata
from src.core.writer import write_delta_table

logger = get_logger("MainOrchestrator")

def run_pipeline(config_path: str, env_catalog: str):
    start_time = time.time()
    logger.info("=================== STARTING DATA PIPELINE ===================")
    
    try:
        config = load_config(config_path, env_catalog)
        spark = get_spark_session()
        pipeline_name = config.get("name", "unknown_pipeline")
        
        # 1. Define Unity Catalog Table Names
        table_ok = f"{env_catalog}.data_ingestion.person_standard_ok"
        table_ko = f"{env_catalog}.data_ingestion.person_standard_ko"
        
        # 2. Define Checkpoint Paths (These still live in the Volume)
        checkpoint_ok = f"/Volumes/{env_catalog}/data_ingestion/landing_zone/checkpoints/ok"
        checkpoint_ko = f"/Volumes/{env_catalog}/data_ingestion/landing_zone/checkpoints/ko"
        
        # Helper function to safely count Delta tables natively
        def get_table_count(t_name: str) -> int:
            try:
                return spark.table(t_name).count()
            except Exception:
                return 0
                
        # Capture initial row counts
        ok_initial = get_table_count(table_ok)
        ko_initial = get_table_count(table_ko)
        
        # Ingest via Auto Loader
        source_df = read_source_data(spark, config["sources"][0])
        logger.info("Auto Loader stream initialized.")
        
        # Transformations & Validations
        source_df_with_time = add_metadata(source_df)
        df_ok, df_ko = apply_validations(source_df_with_time, config["transformations"])
        
        # Start the Delta writers (Writing to Unity Catalog Tables)
        query_ok, _ = write_delta_table(df_ok, table_ok, checkpoint_ok, "ok")
        query_ko, _ = write_delta_table(df_ko, table_ko, checkpoint_ko, "ko")
        
        # Wait for streams to finish
        logger.info("Awaiting stream termination (Processing Incremental Batch)...")
        query_ok.awaitTermination()
        query_ko.awaitTermination()

        # Gold Layer
        logger.info("Building Gold Layer business aggregations...")
        gold_table_name = f"{env_catalog}.data_ingestion.person_gold_office_stats"
        silver_df = spark.table(table_ok)
        
        gold_df = silver_df.groupBy("office").agg(
            count("*").alias("total_employees"),
            spark_round(avg("age"), 1).alias("average_age")
        )
        gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table_name)
        logger.info(f"Gold Layer successfully updated: {gold_table_name}")
        
        # Final row counts
        ok_count = get_table_count(table_ok) - ok_initial
        ko_count = get_table_count(table_ko) - ko_initial
        logger.info(f"Pipeline streams terminated successfully. Processed Valid: {ok_count} rows | Rejected: {ko_count} rows.")
        
        # Liquid Clustering
        spark.sql(f"ALTER TABLE {table_ok} CLUSTER BY (office)")
        spark.sql(f"ALTER TABLE {table_ko} CLUSTER BY (office)")
        
        # Audit Log
        end_time = time.time()
        audit_data = [(pipeline_name, env_catalog, ok_count, ko_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), round(end_time - start_time, 2))]
        audit_schema = "pipeline_name STRING, environment STRING, rows_ok INT, rows_ko INT, execution_timestamp STRING, duration_seconds DOUBLE"
        spark.createDataFrame(audit_data, schema=audit_schema).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{env_catalog}.data_ingestion.pipeline_audit_log")
        
    except Exception as e:
        logger.critical(f"Pipeline failed critically: {e}")
        raise e

if __name__ == "__main__" or "DATABRICKS_RUNTIME_VERSION" in os.environ:
    try:
        env_catalog = dbutils.widgets.get("env_catalog")
    except Exception:
        env_catalog = "dev_bootcamp"

    config_location = os.path.join(project_root, "metadata", "dataflows.json")
    run_pipeline(config_location, env_catalog)