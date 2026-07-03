# Databricks notebook source
import sys
import os
import time
from datetime import datetime
from pyspark.sql.functions import count, avg, round

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
        
        # Helper function to safely count Delta tables (returns 0 if table doesn't exist yet)
        def get_table_count(path: str) -> int:
            try:
                return spark.read.format("delta").load(path).count()
            except Exception:
                return 0
                
        # Get target paths from config
        path_ok = config["sinks"][0]["paths"][0]
        path_ko = config["sinks"][1]["paths"][0]
        
        # Capture initial row counts BEFORE streams start
        ok_initial = get_table_count(path_ok)
        ko_initial = get_table_count(path_ko)
        
        # Ingest via Auto Loader (Stream)
        source_df = read_source_data(spark, config["sources"][0])
        logger.info("Auto Loader stream initialized.")
        
        # Transformations & Validations
        source_df_with_time = add_metadata(source_df)
        df_ok, df_ko = apply_validations(source_df_with_time, config["transformations"])
        
        # Start the Delta writers
        query_ok, _ = write_delta_table(df_ok, config["sinks"][0], "ok")
        query_ko, _ = write_delta_table(df_ko, config["sinks"][1], "ko")
        
        # Wait for Auto Loader to process all new files
        logger.info("Awaiting stream termination (Processing Incremental Batch)...")
        query_ok.awaitTermination()
        query_ko.awaitTermination()

        # Gold Layer - Business Aggregations (Batch Execution)
        logger.info("Building Gold Layer business aggregations...")
        gold_table_name = f"{env_catalog}.data_ingestion.person_gold_office_stats"
        silver_df = spark.read.format("delta").load(path_ok)
        
        # Perform the aggregation
        gold_df = silver_df.groupBy("office").agg(
            count("*").alias("total_employees"),
            round(avg("age"), 1).alias("average_age")
        )
        
        # Overwrite the Gold table in Unity Catalog
        gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table_name)
        logger.info(f"Gold Layer successfully updated: {gold_table_name}")
        
        # Extract exact row counts by diffing the Delta Lake storage layer
        ok_final = get_table_count(path_ok)
        ko_final = get_table_count(path_ko)
        
        ok_count = ok_final - ok_initial
        ko_count = ko_final - ko_initial
        
        logger.info(f"Pipeline streams terminated successfully. Processed Valid: {ok_count} rows | Rejected: {ko_count} rows.")
        
        # Apply Liquid Clustering dynamically via Spark SQL now that data is written
        spark.sql(f"ALTER TABLE delta.`{path_ok}` CLUSTER BY (office)")
        spark.sql(f"ALTER TABLE delta.`{path_ko}` CLUSTER BY (office)")
        logger.info("Liquid Clustering natively enabled on destination tables by column: office")
        
        # Write to the Audit Log Table for traceability
        end_time = time.time()
        duration_sec = round(end_time - start_time, 2)
        
        audit_data = [(
            pipeline_name, 
            env_catalog, 
            ok_count, 
            ko_count, 
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            duration_sec
        )]
        audit_schema = "pipeline_name STRING, environment STRING, rows_ok INT, rows_ko INT, execution_timestamp STRING, duration_seconds DOUBLE"
        
        audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
        audit_table_name = f"{env_catalog}.data_ingestion.pipeline_audit_log"
        
        audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table_name)
        logger.info(f"Audit metrics successfully appended to catalog table: {audit_table_name}")
        
    except Exception as e:
        logger.critical(f"Pipeline failed critically: {e}")
        raise e
        
    logger.info(f"=================== PIPELINE FINISHED IN {round(time.time() - start_time, 2)} SECONDS ===================")

if __name__ == "__main__" or "DATABRICKS_RUNTIME_VERSION" in os.environ:
    try:
        env_catalog = dbutils.widgets.get("env_catalog")
    except NameError:
        env_catalog = "dev_bootcamp"
    except Exception:
        env_catalog = "dev_bootcamp"

    config_location = os.path.join(project_root, "metadata", "dataflows.json")
    run_pipeline(config_location, env_catalog)