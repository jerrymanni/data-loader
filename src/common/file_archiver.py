from common.logger import get_logger
import datetime
from pathlib import Path

def archive_file(source_file):
    logger = get_logger()
    logger.info("Generating timestap for file archiving.")
    logger.info(f"Trying to move file: {source_file}")
    now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    try:
        file = Path(f"./data/landing/{source_file}.csv")
        file.rename(f"./data/archive/{source_file}_{now}.csv")
        logger.info("File archived!")
    except Exception as e:
        logger.error("Failed to move file to archive!")
        raise NameError("Could not move the file.")