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
    data = [
        ("User_1", 25, "MADRID"),
        ("User_2", None, "RIO"),
        ("User_3", 30, ""),
        ("User_4", 16, "MADRID"),
        ("User_5", 70, "RIO")  
    ]
    schema = ["name", "age", "office"]
    mock_df = spark.createDataFrame(data, schema)
    
    mock_transformations = [
        {
            "type": "validate_fields",
            "params": {
                "validations": [
                    {
                        "field": "age", 
                        "validations": ["notNull", "isAdult"],
                        "sql_expressions": [{"error_code": "not_retirement_age", "expression": "age < 65"}]
                    },
                    {"field": "office", "validations": ["notEmpty"]}
                ]
            }
        }
    ]
    
    df_ok, df_ko = apply_validations(mock_df, mock_transformations)
    
    # Check Row Counts (Only User_1 should pass)
    assert df_ok.count() == 1, "Exactly one row should pass validation."
    assert df_ko.count() == 4, "Exactly four rows should fail validation."
    
    # Check Dictionary Accuracy
    ko_rows = df_ko.collect()
    for row in ko_rows:
        error_map = row["arraycoderrorbyfield"]
        if row["name"] == "User_2":
            assert error_map["age"] == "notNull"
        elif row["name"] == "User_3":
            assert error_map["office"] == "notEmpty"
        elif row["name"] == "User_4":
            assert error_map["age"] == "isAdult"
        elif row["name"] == "User_5":
            assert error_map["age"] == "not_retirement_age" # Caught by the SQL expression