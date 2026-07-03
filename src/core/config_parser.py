import json
import os
from src.utils.logger import get_logger

logger = get_logger("ConfigParser")

def validate_metadata_schema(config: dict):
    """
    Strictly validates that the JSON contains all required pipeline keys 
    before allowing the engine to proceed.
    """
    required_keys = ["name", "sources", "transformations", "sinks"]
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        logger.error(f"Metadata validation failed. Missing keys: {missing_keys}")
        raise ValueError(f"Invalid JSON metadata. Missing: {missing_keys}")
        
    if not isinstance(config.get("sources"), list) or len(config.get("sources")) == 0:
        logger.error("The 'sources' array is empty or invalid.")
        raise ValueError("Pipeline must have at least one source configuration.")
        
    logger.info("Metadata JSON schema successfully validated.")

def load_config(file_path: str, env_catalog: str = "dev_bootcamp") -> dict:
    """Reads and validates the JSON metadata contract, injecting the target environment."""
    logger.info(f"Attempting to load metadata configuration from: {file_path}")
    logger.info(f"Target Environment Catalog: {env_catalog}")
    
    if not os.path.exists(file_path):
        logger.error(f"Configuration file not found at {file_path}")
        raise FileNotFoundError(f"Missing config: {file_path}")
        
    try:
        # Read the file
        with open(file_path, 'r') as file:
            raw_json = file.read()
            
        # Inject the dynamic catalog environment
        raw_json = raw_json.replace("{env_catalog}", env_catalog)
        
        # Parse the string into a dictionary
        config = json.loads(raw_json)
        
        # Run our new strict validation check
        validate_metadata_schema(config)
        
        logger.info(f"Successfully loaded configuration for pipeline: {config.get('name')}")
        return config
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in {file_path}. Error: {e}")
        raise