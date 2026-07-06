import os

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from src.utils.logger import get_logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logger = get_logger("Reader")

def get_spark_session():
    """
    Initializes a Spark session dynamically depending on whether it's running 
    locally via Databricks Connect or natively in the Databricks Cloud.
    """
    logger.info("Initializing Spark Session...")
    
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        # 1. We are running inside a Databricks Job/Notebook natively
        spark = SparkSession.builder.getOrCreate()
        logger.info("Native Databricks Spark Session successfully established.")
        return spark
    else:
        # 2. We are running locally from VS Code via Databricks Connect
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
        logger.info("Databricks Connect (Serverless) Session successfully established.")
        return spark


def read_source_data(spark, source_config: dict):
    source_path = source_config.get("path").replace("/*", "")
    
    logger.info(f"Setting up Databricks Auto Loader for path: {source_path}")
    
    # Define the Strict Enterprise Schema
    expected_schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("office", StringType(), True)
    ])
    
    # Define where Auto Loader saves its state
    schema_checkpoint_dir = f"{source_path}/_schema_checkpoint"
    
    # Read as an Incremental Stream
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", schema_checkpoint_dir) \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .schema(expected_schema) \
        .load(source_path)
        
    return df