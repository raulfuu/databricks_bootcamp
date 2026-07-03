import pytest
from databricks.connect import DatabricksSession
from src.core.validator import apply_validations

# --- 1. Pytest Fixture to provide a Databricks Connect Session ---
@pytest.fixture(scope="session")
def spark():
    """Creates a remote Spark session using Databricks Serverless compute."""
    return DatabricksSession.builder.serverless(True).getOrCreate()

# --- 2. The Integration Test ---
def test_apply_validations(spark):
    """Tests the dynamic error mapping and DataFrame splitting logic."""
    data = [
        ("User_1", 25, "MADRID"),
        ("User_2", None, "RIO"),
        ("User_3", 30, "")
    ]
    schema = ["name", "age", "office"]
    mock_df = spark.createDataFrame(data, schema)
    
    # Mock metadata matching our data
    mock_transformations = [
        {
            "type": "validate_fields",
            "params": {
                "validations": [
                    {"field": "age", "validations": ["notNull"]},
                    {"field": "office", "validations": ["notEmpty"]}
                ]
            }
        }
    ]
    
    # Run your function
    df_ok, df_ko = apply_validations(mock_df, mock_transformations)
    
    # --- Assertions ---
    # 1. Check Row Counts
    assert df_ok.count() == 1, "Exactly one row should pass validation."
    assert df_ko.count() == 2, "Exactly two rows should fail validation."
    
    # 2. Check Schema
    assert "arraycoderrorbyfield" in df_ko.columns, "Error trace column must be added to KO dataframe."
    
    # 3. Check Dictionary Accuracy
    ko_rows = df_ko.collect()
    for row in ko_rows:
        error_map = row["arraycoderrorbyfield"]
        if row["name"] == "User_2":
            assert "age" in error_map and error_map["age"] == "notNull", "Failed to map age notNull error."
        elif row["name"] == "User_3":
            assert "office" in error_map and error_map["office"] == "notEmpty", "Failed to map office notEmpty error."