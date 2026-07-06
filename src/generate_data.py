import pyspark.sql.functions as F
from src.core.reader import get_spark_session
from src.utils.logger import get_logger

logger = get_logger("DataGenerator")

def generate_dataset(env_catalog: str, num_rows: int):
    spark = get_spark_session()
    logger.info(f"Starting generation of {num_rows} rows for [{env_catalog}]...")

    # Generate rows
    df = spark.range(0, num_rows)

    df_generated = df.withColumn(
        "name", F.concat(F.lit("User_"), F.col("id").cast("string"))
    ).withColumn(
        "age", F.when(F.rand() < 0.1, F.lit(None)).otherwise((F.rand() * 75 + 10).cast("int"))
    ).withColumn(
        "office", F.when(F.rand() < 0.1, F.lit("")).otherwise(
            F.when(F.rand() < 0.5, F.lit("MADRID")).otherwise(F.lit("RIO"))
        )
    ).drop("id")

    # Dynamic path based on the catalog
    out_path = f"/Volumes/{env_catalog}/data_ingestion/landing_zone/input_performance/events/person"
    logger.info(f"Writing dataset to {out_path}...")
    
    # Write the data
    df_generated.coalesce(10).write.mode("append").json(out_path) # append / overwrite
    logger.info(f"Successfully generated data for {env_catalog}.")

if __name__ == "__main__":
    # Populate DEV with a tiny dataset for fast testing (1,000 rows)
    generate_dataset(env_catalog="dev_bootcamp", num_rows=1000)
    
    # Populate PROD with the massive performance dataset (5,000,000 rows)
    generate_dataset(env_catalog="prod_bootcamp", num_rows=5000000)