from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, date_format
from src.utils.logger import get_logger

logger = get_logger("Transformer")

def add_metadata(df: DataFrame) -> DataFrame:
    """
    Appends the 'dt' processing timestamp to the DataFrame.
    Formatted exactly to yyyy-MM-dd HH:mm:ss.
    """
    logger.info("Adding 'dt' timestamp metadata column.")
    
    df_transformed = df.withColumn(
        "dt", 
        date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")
    )
    
    return df_transformed