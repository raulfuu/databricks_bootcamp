import pytest
from databricks.connect import DatabricksSession
from src.core.validator import apply_validations

@pytest.fixture(scope="session")
def spark():
    """Creates a remote Spark session using Databricks Serverless compute."""
    return DatabricksSession.builder.serverless(True).getOrCreate()

# Integration Test
def test_apply_validations(spark):
    """Tests the dynamic error mapping and DataFrame splitting logic."""
    # Added User_4 who is under 18!
    data = [
        ("User_1", 25, "MADRID"),
        ("User_2", None, "RIO"),
        ("User_3", 30, ""),
        ("User_4", 16, "MADRID")  
    ]
    schema = ["name", "age", "office"]
    mock_df = spark.createDataFrame(data, schema)
    
    # Updated mock metadata to include isAdult
    mock_transformations = [
        {
            "type": "validate_fields",
            "params": {
                "validations": [
                    {"field": "age", "validations": ["notNull", "isAdult"]},
                    {"field": "office", "validations": ["notEmpty"]}
                ]
            }
        }
    ]
    
    df_ok, df_ko = apply_validations(mock_df, mock_transformations)
    
    # Check Row Counts (Only User_1 should pass now)
    assert df_ok.count() == 1, "Exactly one row should pass validation."
    assert df_ko.count() == 3, "Exactly three rows should fail validation."
    
    # Check Dictionary Accuracy
    ko_rows = df_ko.collect()
    for row in ko_rows:
        error_map = row["arraycoderrorbyfield"]
        if row["name"] == "User_2":
            assert "age" in error_map and error_map["age"] == "notNull"
        elif row["name"] == "User_3":
            assert "office" in error_map and error_map["office"] == "notEmpty"
        elif row["name"] == "User_4":
            assert "age" in error_map and error_map["age"] == "isAdult" # Tests the new rule!