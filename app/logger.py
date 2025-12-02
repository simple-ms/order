import logging
import sys
from pythonjsonlogger import jsonlogger


def get_logger(service_name: str):
    """
    Create a structured JSON logger for the service.
    
    Args:
        service_name: Name of the service for log identification
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(service_name)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
    return logger


logger = get_logger("order-service")
