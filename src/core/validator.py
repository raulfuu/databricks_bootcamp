from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, struct, array, size, map_from_entries
from pyspark.sql.functions import filter as spark_filter
from src.utils.logger import get_logger

logger = get_logger("Validator")

def apply_validations(df: DataFrame, transformations_config: list) -> tuple[DataFrame, DataFrame]:
    """
    Dynamically builds an error map for every row.
    Splits the data based on whether the error map is empty.
    """
    val_config = next((t for t in transformations_config if t.get("type") == "validate_fields"), None)
    
    if not val_config:
        return df, df.sparkSession.createDataFrame([], df.schema)
        
    rules = val_config.get("params", {}).get("validations", [])
    error_structs = []
    
    for rule in rules:
        field = rule.get("field")
        for val_type in rule.get("validations", []):

            if val_type == "notNull":
                is_valid = col(field).isNotNull()
            elif val_type == "notEmpty":
                is_valid = col(field).isNotNull() & (col(field) != lit(""))
            else:
                continue
                
            # If invalid, create a struct(key=field, value=error_type). If valid, return NULL.
            err_struct = when(~is_valid, struct(lit(field).alias("k"), lit(val_type).alias("v")))
            error_structs.append(err_struct)

    if error_structs:
        # Combine all possible errors into a single array
        all_errors = array(*error_structs)
        
        # Filter out the NULLs (the rules that passed)
        actual_errors = spark_filter(all_errors, lambda x: x.isNotNull())
        
        # Native Dictionary column
        error_map_col = map_from_entries(actual_errors)
        df_evaluated = df.withColumn("arraycoderrorbyfield", error_map_col)
        
        # Good data has 0 errors (we drop the empty error column to keep it clean)
        df_ok = df_evaluated.filter(size(col("arraycoderrorbyfield")) == 0).drop("arraycoderrorbyfield")
        
        # Bad data has > 0 errors (we keep the error column)
        df_ko = df_evaluated.filter(size(col("arraycoderrorbyfield")) > 0)
        
        logger.info("Successfully mapped dynamic error traces to rejected records.")
        return df_ok, df_ko
    else:
        return df, df.sparkSession.createDataFrame([], df.schema)