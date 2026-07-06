from pyspark.sql import DataFrame
from src.utils.logger import get_logger

logger = get_logger("Writer")

def write_delta_table(df: DataFrame, table_name: str, checkpoint_path: str, stream_name: str) -> tuple:
    """
    Writes a Streaming DataFrame directly to a Unity Catalog Managed Table.
    """
    logger.info(f"Initializing Auto Loader Write Stream to Unity Catalog Table: {table_name}")
    
    try:
        query = (df.writeStream
                 .format("delta")
                 .option("checkpointLocation", checkpoint_path)
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .outputMode("append")
                 .toTable(table_name))
        
        logger.info(f"Stream '{stream_name}' successfully started for: {table_name}")
        return query, table_name
        
    except Exception as e:
        logger.error(f"Failed to start stream to {table_name}. Error: {e}")
        raise