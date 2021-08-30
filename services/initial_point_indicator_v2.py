#%%
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import minmax_scale
#%%

def initial_point_indicator(df: 'table') -> 'Table':
    """
    |--------------------------------------------------------------------------
    |   TASK: Initial Point Indicator
    |--------------------------------------------------------------------------
    |   INPUTS:
    |       integrated table (from integrate data)
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    |       initial_net_item, initial_discount and item_sold_std
    |       based on DKP, day flag and discount type
    |--------------------------------------------------------------------------
    
    """ 
    aggregation_level = ["day_flag", "DKP", "DKPC" , "tracking_type" , "discount_type"]

    def aggregation_measurs(x):
        d = dict()
        d['initial_item_sold'] = x["Net_Items"].mean()
        d['total_item_sold'] = x["Net_Items"].sum()
        d['live_hours'] = x['live_hours'].sum()
        d['Item Discount Net'] = x["Item Discount Net"].sum()
        d["NMV 1"] = x["NMV 1"].sum()
        d['std_item_sold'] = np.std(x['Net_Items']) 
        d['day'] = x['Date'].nunique()
        return pd.Series(d, index = ['initial_item_sold','total_item_sold','live_hours','Item Discount Net',"NMV 1",'std_item_sold','day'])

    aggregate_df = df.groupby(by = aggregation_level).apply(aggregation_measurs).reset_index()
    aggregate_df['initial_discount_percent'] = aggregate_df['Item Discount Net'] / (aggregate_df['Item Discount Net'] + aggregate_df["NMV 1"])
    aggregate_df['initial_item_sold_hourly_stock'] = aggregate_df["total_item_sold"] / (aggregate_df['live_hours'] / 24)  

    return aggregate_df
    
