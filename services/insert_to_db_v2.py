# %%
import pandas as pd
from os import path
import pandahouse as ph
import db_connection as dc
import datetime


# %%

def insert_to_db(df: 'table'):
    """
    |--------------------------------------------------------------------------
    | Insert to Database (for usage of dashboard)
    |--------------------------------------------------------------------------
    |   TASK of insert_to_db:
    | insert data ro database
    |--------------------------------------------------------------------------
    |   INPUTS:  
    | df: table to insert to database
    |--------------------------------------------------------------------------
    |   OUTPUTS: None
    |--------------------------------------------------------------------------
    """

    df["recorded_at"] = datetime.datetime.now()
    df = df.astype({'recorded_at': 'datetime64[s]'})
    df = df.astype({'recorded_at': 'str'})

    connection = dict(database='database_name',
                      host='ip_and_port_of_database',
                      user='username',
                      password='password')
    df.set_index('DKPC', inplace=True)
    ph.to_clickhouse(df, 'optimized_discounts', connection=connection)


# %%
def insert_to_db_bi(df: 'table', append_or_replace: 'str'):
    """
    |--------------------------------------------------------------------------
    | Insert to Database (for usage of dashboard)
    |--------------------------------------------------------------------------
    |   TASK of insert_to_db:
    | insert data ro database
    |--------------------------------------------------------------------------
    |   INPUTS:  
    | df: table to insert to database
    |--------------------------------------------------------------------------
    |   OUTPUTS: None
    |--------------------------------------------------------------------------
    """

    df["recorded_at"] = datetime.datetime.now()
    job = dc.job()
    job.source_selection(source='mssql_36', schema='dbo', table='optimized_discounts')
    job.job_insert(df, append_or_replace=append_or_replace)
