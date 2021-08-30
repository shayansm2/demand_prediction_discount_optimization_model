#%%
import pandas as pd
import numpy as np
import jdatetime
#%%
def sales_aggregation(df : 'table',
                      aggregation_level : 'array' ) -> 'table':
    """
    |--------------------------------------------------------------------------
    | Sales Aggregation (used for F_factor)
    |--------------------------------------------------------------------------
    |   TASK of Sales Aggregation:
    | aggregate the integration result based on the given aggregation array
    |--------------------------------------------------------------------------
    |   INPUTS:
    | df: result table from integrated data
    | aggregation_level: a list of all column names of the discount_bucket_result 
    |                    data frame which is going to be grouped by on 
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: sales data aggregation on discount_bucket_result and other factors
    |--------------------------------------------------------------------------
    """

    aggregation_level += ['discount_type','day_flag','discount_bucket','min','max','time_period', 'tracking_type']

    def aggregation_measurs(x):
        d = dict()
        d['sales_in_cluster'] = x['Net_Items'].sum()
        d['day_with_sales_in_cluster'] = x['Date'].nunique()
        d['hours_with_asrt_in_cluster'] = x['live_hours'].sum()
        d['NMV'] = x['NMV 1'].sum()
        d['Item_discount_net'] = x['Item Discount Net'].sum()
        d['count_of_dkp'] = x['DKP'].nunique()
        return pd.Series(d, index = ['sales_in_cluster','day_with_sales_in_cluster','hours_with_asrt_in_cluster','NMV','Item_discount_net','count_of_dkp'])

    aggregate_df = df.groupby(by = aggregation_level).apply(aggregation_measurs).reset_index()
    aggregate_df['discount_in_cluster'] = (aggregate_df['Item_discount_net']) / (aggregate_df['NMV']+aggregate_df['Item_discount_net'])
    aggregate_df['sales_potential'] = aggregate_df['sales_in_cluster'] / (aggregate_df['hours_with_asrt_in_cluster'] /24)

    return aggregate_df