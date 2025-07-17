import logging

def get_logger():
    logger = logging.getLogger(__name__)

    logging.basicConfig(filename="./logs/logs.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    return logger