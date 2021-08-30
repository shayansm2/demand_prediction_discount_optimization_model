# %%
import pandas as pd
from os import path
import pandahouse as ph


# %%

def get_hourly_stock_data(category: 'string',
                          weights: 'table',
                          prev_days: 'int' = 30) -> 'table':
    """
    |--------------------------------------------------------------------------
    | Get Hourly Stock Data (from DB)
    |--------------------------------------------------------------------------
    |   TASK of get_asrt_data:
    | get data from BI DB
    |--------------------------------------------------------------------------
    |   INPUTS:  
    | execute_sql : if TRUE execute the SQL and update the sales file
    | prev_days: get data of last prev_days days
    | file directory : where to save the file
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: returnes ASRT table
    |--------------------------------------------------------------------------
    """

    query = '''
    select dkp, dkpc,
    'Danesh' as warehouse,
    toDate(created_at) as created_date, 
    toHour(created_at) as created_hour,
    if(is_live = 1 and serial_stock > 0 , 1, 0) as is_live
    from dk_warehouse.FMCG_Hourly_Stock
    where cat = ''' + "'" + str(category) + "'" + '''
    and created_at >= date_add(DAY, -''' + str(prev_days) + ''', now())

    UNION ALL

    select dkp, dkpc,
    'Shad Abad' as warehouse,
    toDate(created_at) as created_date, 
    toHour(created_at) as created_hour,
    if(is_live = 1 and batch_stock > 0 , 1, 0) as is_live
    from dk_warehouse.FMCG_Hourly_Stock
    where cat = ''' + "'" + str(category) + "'" + '''
    and created_at >= date_add(DAY, -''' + str(prev_days) + ''', now())'''

    connection = dict(database='database_name',
                      host='ip_and_port_of_database',
                      user='username',
                      password='password')
    df = ph.read_clickhouse(query, connection=connection)

    df = pd.merge(left=df, right=weights, left_on="created_hour", right_on="Hour")
    df["stock_weight"] = df["is_live"] * df["weight"]

    df.drop(columns=["created_hour", "is_live", "Hour"], inplace=True)
    df = df.groupby(by=["dkpc", "dkp", "warehouse", "created_date"]).sum().reset_index()
    df["live_ratio"] = df["stock_weight"] / df["weight"]
    df["live_hours"] = 24 * df["live_ratio"]
    df.drop(columns=["stock_weight", "weight", "live_ratio"], inplace=True)
    df.rename(columns={"created_date": "date"}, inplace=True)

    return df
