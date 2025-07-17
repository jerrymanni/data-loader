

import yaml
from pathlib import Path
from sqlalchemy import text
from common.logger import get_logger
from common.database import create_db_engine

class DWLoader:

    def __init__(self, table):
        self.table = table
        self.logger = get_logger()
        self.db_engine = create_db_engine()

    def _read_schema(self):
        p = Path(f"./data/schemas/{self.table}.yaml")
        with open(p) as f:
             y = yaml.safe_load(f)
        return y
    
    def _create_cols(self):
        schema = self._read_schema()

        cols = ','.join(list(map(lambda x: "stg." + x, list(schema["columns"].keys()))))
        key_cols = ','.join(["stg." + x for x in schema["key"]])
        data_cols = ','.join(["stg." + x for x in schema["data"]])

        return {"cols": cols, "key_cols": key_cols, "data_cols": data_cols}
    
    def create_insert_query(self):
        cols, key_cols, data_cols = self._create_cols().values()

        return  f"""    
                with data as (
                select
                {cols},
                insert_ts,
                md5(concat_ws('|', {key_cols})) as hash_key,
                md5(concat_ws('|', {data_cols})) as datahash
                from stg.{self.table} stg
                )
                insert into dw.{self.table} 
                ({cols.replace("stg.", "")}, insert_ts, hash_key, datahash)
                select
                    {cols},
                    stg.insert_ts,
                    stg.hash_key,
                    stg.datahash
                from data stg
                left join dw.{self.table} dw
                    on dw.hash_key = stg.hash_key
                    and dw.is_current = True
                where dw.datahash <> stg.datahash 
                or dw.datahash is null
                ;
                """
        
    def create_update_query(self):
        return f"""
                with src as (
                select hash_key, insert_ts, row_number() over (partition by hash_key order by insert_ts desc) as rn
                from dw.{self.table}
                where is_current = True
                )
                update dw.{self.table} t
                set is_current = false
                from (select hash_key, insert_ts from src where rn <> 1) s
                where s.hash_key = t.hash_key and s.insert_ts = t.insert_ts
                ;
                """

    def insert_new_rows(self):
        try:
            self.logger.info("Starting inserting new rows...")
            with self.db_engine.connect() as e:
                e.execute(text(self.create_insert_query()))
                e.commit()
        except Exception as e:
            self.logger.error(f"Insert into dw.{self.table} failed with message: {e}")
            raise Exception(f"Insert into dw.{self.table} failed with message: {e}")
        self.logger.info("New rows inserted!")

        return "Insert succesful"
    
    def update_is_current(self):
        try:
            self.logger.info("Starting updating is_current.")
            with self.db_engine.connect() as e:
                e.execute(text(self.create_update_query()))
                e.commit()
        except Exception as e:
            self.logger.error(f"Updating table dw.{self.table} failed with message: {e}")
            raise Exception(f"Updating table dw.{self.table} failed with message: {e}")
        self.logger.info("Update finished!")

        return "update succesful"
