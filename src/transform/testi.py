
import yaml
from pathlib import Path

table = "daily_sales"
p = Path(f"../data/schemas/{table}.yaml")
with open(p) as f:
    y = yaml.safe_load(f)

cols = ','.join(list(map(lambda x: "stg." + x, list(y["columns"].keys()))))
key_cols = ','.join(["stg." + x for x in y["key"]])
data_cols = ','.join(["stg." + x for x in y["data"]])

insert_query = f"""    
    with data as (
	select
	{cols},
	md5(concat_ws('|', {key_cols})) as hash_key,
    md5(concat_ws('|', {data_cols})) as datahash
	from stg.{table} stg
    )
    insert into dw.{table} 
    ({cols}, hash_key, datahash)
    select
        {cols}
        stg.hash_key,
        stg.datahash
    from data stg
    left join dw.{table} dw
        on dw.hash_key = stg.hash_key
        and dw.is_current = True
    where dw.datahash <> stg.datahash 
    ;
    """