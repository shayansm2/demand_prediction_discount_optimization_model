#%%
import pandas as pd
import numpy as np
import jdatetime
#%%
def add_hourly_stock(df : 'table',
                    df_hourly_stock: 'table') -> 'table':
    """
    |--------------------------------------------------------------------------
    | Add Hourly Stock 
    |--------------------------------------------------------------------------
    |   Task of add hourly stock:
    | it will join sales data and hourly stock data
    |--------------------------------------------------------------------------
    |    INPUTS:
    | df: table sale (from get_sales_data)
    | df_hourly_stock: table hourly stock (from get_hourly_stock_data)
    |--------------------------------------------------------------------------
    |   OUTPUT:
    | integrated table of sales and hourly stock
    |--------------------------------------------------------------------------
    """

    def jalali2standard(ymd):
        y = int(str(ymd)[:4])
        m = int(str(ymd)[4:6])
        d = int(str(ymd)[6:])
        gregorian_date = jdatetime.date(y,m,d).togregorian()
        return str(gregorian_date)
    
    # print(df.columns)
    # print(df_hourly_stock.columns)
    df = df.astype({"DKP":"int64", "DKPC":"int64"})
    df_hourly_stock = df_hourly_stock.astype({"dkp":"int64", "dkpc":"int64"})

    df_hourly_stock["Business Type"] = "Retail"
    df_hourly_stock = df_hourly_stock[df_hourly_stock["live_hours"] != 0]

    dim_date_columns = ["Year","Season","Month","Date","Day Number Of Week","Day Number Of Month"]
    dim_date = df[dim_date_columns].drop_duplicates()
    dim_date.reset_index(inplace = True)
    dim_date.drop(columns = ["index"],inplace = True)
    dim_date["gregorian_date"] = dim_date["Date"].apply(lambda x: jalali2standard(x))

    dim_product_columns = ["DKP", "DKPC", "Tracking Type Desc" , "Brand", "Leaf Cat"]
    dim_product = df[dim_product_columns].drop_duplicates()
    dim_product.reset_index(inplace = True)
    dim_product.drop(columns = ["index"],inplace = True)

    df["gregorian_date"] = df["Date"].apply(lambda x: jalali2standard(x))
    df_fact = df[["DKP","DKPC", "Tracking Type Desc","Business Type","NMV 1","# Net Items","Item Discount Net",'Incredible Flag',
                     'Promotion Flag',"NMV 3","Gross Margin 3","COGS","Gross Margin Ratio Of NMV","gregorian_date"]]

    new_df = pd.merge(left = df_fact,
                    right = df_hourly_stock,
                    how = "outer",
                    left_on = ["DKP","Business Type","gregorian_date" ,"DKPC", "Tracking Type Desc"], 
                    right_on = ["dkp", "Business Type","date", 'dkpc', 'warehouse'])

    new_df.iloc[new_df[new_df["DKP"].isnull()].index,0] = new_df[new_df["DKP"].isnull()]['dkp']
    new_df.iloc[new_df[new_df["DKPC"].isnull()].index,1] = new_df[new_df["DKPC"].isnull()]['dkpc']
    new_df.iloc[new_df[new_df["Tracking Type Desc"].isnull()].index,2] = new_df[new_df["Tracking Type Desc"].isnull()]['warehouse']
    new_df.iloc[new_df[new_df["gregorian_date"].isnull()].index,13] = new_df[new_df["gregorian_date"].isnull()]['date']
    new_df.drop(columns = ["dkp","date","dkpc", "warehouse"], inplace = True)
    new_df = new_df.astype({"DKP":"int64", "DKPC":"int64"})

    new_df["live_hours"].fillna(24, inplace = True)
    new_df.fillna(0, inplace = True)

    new_df = pd.merge(left = new_df,
                    right = dim_product,
                    how = "left",
                    on = ["DKP", "DKPC", "Tracking Type Desc"])

    new_df = pd.merge(left = new_df,
                    right = dim_date,
                    how = "inner",
                    on = "gregorian_date")

    return new_df