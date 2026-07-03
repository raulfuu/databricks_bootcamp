import logging

def get_logger(name: str = "PipelineEngine") -> logging.Logger:
    """
    Creates and returns a standardized logger instance.
    Prevents duplicate logs if called multiple times.
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        
        # Format: [Time] - [Module] - [Level] - Message
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        
    return logger