from pyspark.sql import DataFrame
from src.utils.logger import get_logger

logger = get_logger("Writer")

def write_delta_table(df: DataFrame, sink_config: dict, stream_name: str) -> tuple:
    """
    Writes a Streaming DataFrame to a Delta table within Unity Catalog volumes/tables.
    Optimized natively with Delta Lake features like mergeSchema.
    """
    paths = sink_config.get("paths", [])
    if not paths:
        logger.error(f"No sink path configured for target {sink_config.get('name')}")
        raise ValueError("Missing sink destination path.")
        
    target_path = paths[0]
    fmt = sink_config.get("format", "DELTA").lower()
    
    logger.info(f"Initializing Auto Loader Write Stream to: {target_path}")
    checkpoint_path = f"{target_path}/_checkpoints"
    
    try:
        # Build the write stream (Incremental Batch via Trigger.AvailableNow)
        query = (df.writeStream
                 .format(fmt)
                 .option("checkpointLocation", checkpoint_path)
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .outputMode("append")
                 .start(target_path))
        
        logger.info(f"Stream '{stream_name}' successfully started for: {target_path}")
        return query, target_path
        
    except Exception as e:
        logger.error(f"Failed to start stream to {target_path}. Error: {e}")
        raise