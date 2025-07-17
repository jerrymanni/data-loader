from pipelines.base_pipeline import base_pipeline
import pandas as pd
from common.database import create_db_engine
from sqlalchemy import create_engine, text


def daily_sales():
    #db_engine = create_engine("postgresql://postgres:mysecretpassword@localhost/myapp")
    db_engine = create_db_engine()
    # Load file until DW layer
    base_pipeline("data", "daily_sales")

    df = pd.read_sql_query("""
                           select 
                           date, 
                           accounting_unit,
                           account,
                           balance,
                           insert_ts
                           from dw.daily_sales
                           where is_current = True
                           """, db_engine)
    
    df["month"] = pd.to_datetime(df["date"]).dt.month
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df.drop("date", axis=1, inplace=True)

    df_grouped = df.groupby(["accounting_unit", "account", "year", "month"]).agg(balance = ("balance", "sum"), insert_ts = ("insert_ts", "max")).reset_index()
    df_grouped.to_sql("temp_daily_sales_monthly", db_engine, schema="work", index=False, if_exists="replace")


    merge_query = text("""
                    merge into publish.daily_sales_monthly as t
                        using (
                        select
                        year,
                        month,
                        accounting_unit,
                        account,
                        balance,
                        insert_ts
                        from work.temp_daily_sales_monthly
                        ) as s
                    on t.year = s.year
                    and t.month = s.month
                    and t.accounting_unit = s.accounting_unit
                    and t.account = s.account
                        
                    when matched then
                    update set
                        balance = s.balance

                    when not matched then
                        insert (year, month, accounting_unit, account, balance, insert_ts)
                        values (s.year, s.month, s.accounting_unit, s.account, s.balance, s.insert_ts)

                   """)

    with db_engine.connect() as conn:
        conn.execute(merge_query)
        conn.commit()
