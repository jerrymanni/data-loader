from sqlalchemy import text
from common.logger import get_logger
from common.database import create_db_engine
import pandas as pd

class STGLoader:

    def __init__(self, file, table, delimiter=";"):
        self.file = file
        self.delimiter = delimiter
        self.table = table
        self.logger = get_logger()
        self.db_engine = create_db_engine()

    def load_file(self):
        self.logger.info("Reading file...")
        try:
            df = pd.read_csv(f"./data/landing/{self.file}", delimiter=self.delimiter)
        except FileNotFoundError:
            self.logger.error("No files found.") 
            raise FileNotFoundError("No files to be loaded")
            
        self.logger.info(f"Row count {df.shape[0]}")
        return df
    
    def truncate_stg(self):
        try:
            with self.db_engine.connect() as e:
                e.execute(text(f"truncate stg.{self.table}"))
                e.commit()
        except Exception as e:
            self.logger.error(f"Truncating stg.{self.table} failed with message: {e}")
            raise Exception(f"Truncating stg.{self.table} failed with message: {e}")
        
    def insert_into_stg(self):
        df = self.load_file()
        df.to_sql(name="daily_sales", con=self.db_engine, schema="stg", if_exists="append", index=False)