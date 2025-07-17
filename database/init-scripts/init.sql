-- Create schemas
create schema stg;
create schema dw;
create schema publish;

-- Create daily_sales staging
create table stg.daily_sales (
    date date,
    accounting_unit varchar(7),
    account varchar(5),
    balance float8,
    insert_ts timestamp default current_timestamp
);

-- Create daily_sales dw
create table dw.daily_sales (
    date date,
    accounting_unit varchar,
    account varchar,
    balance float8,
    insert_ts timestamp,
    is_current boolean default True,
    hash_key varchar(32),
    datahash varchar(32)
);

-- Create dw.daily_sales index
create index pk_dw_daily_sales on dw.daily_sales(date, accounting_unit, account);
create index dw_daily_sales_hash on dw.daily_sales(hash_key);

-- Create daily_sales_monthly publish
create table publish.daily_sales_monthly (
    year int,
    month int,
    accounting_unit varchar,
    account varchar,
    balance float8
);

create index pb_daily_sales_time on publish.daily_sales_monthly(year, month);
create index pb_daily_sales_accounting_unit on publish.daily_sales_monthly(accounting_unit);
-- Grant permissions (if needed)


GRANT SELECT, INSERT, UPDATE, DELETE ON stg.daily_sales TO postgres;
GRANT SELECT, INSERT, UPDATE ON dw.daily_sales TO postgres;