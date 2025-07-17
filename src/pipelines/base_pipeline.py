from common.logger import get_logger
from common.file_archiver import archive_file
from load.dw_load import DWLoader
from extract.stg_load import STGLoader

def base_pipeline(file: str, table: str) -> None:
    table = "daily_sales"
    logger = get_logger()
    logger.info("Starting pipeline!")

    dw_engine = DWLoader(table)
    stg_engine = STGLoader(f"{file}.csv", table)
 
    # Truncate staging
    stg_engine.truncate_stg()
    # Load file to staging
    stg_engine.insert_into_stg()

    # Insert new rows
    dw_engine.insert_new_rows()

    # Update is_current
    dw_engine.update_is_current()

    # Move source file to archive
    archive_file(file)

    logger.info("Pipeline ended!")