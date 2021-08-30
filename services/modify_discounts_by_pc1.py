#%%
import pandas as pd
import numpy as np
#%%

def modify_discounts_by_pc1(df : 'table' ,
                            pc1 : 'table') -> 'table':
    """
    |--------------------------------------------------------------------------
    | Modify Discounts by PC1: 
    |--------------------------------------------------------------------------
    |   TASK of Modify Discounts by PC1: 
    | 1. check whether the suggested discount causes negative pc1
    |       in that case it will reduce the discount so it can have zero pc1
    | 2. check pc1 of product with the pc1 of leafcats
    |       if .... 
    |--------------------------------------------------------------------------
    |   INPUTS: 
    |		
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    |       a table with the optimum discount value 
    |--------------------------------------------------------------------------
    """
    
    def positive_pc1_discount_calculator(rrp, pp, d):
        discount_threshold = 1 - (pp / rrp)
        return min(d, discount_threshold)

    def leafcat_pc1_discount_calculator(rrp, pp, d, pc1):
        acceptable_lower_pc1 = 30000
        model_pc1 = rrp * (1 - d) - pp
        if model_pc1 >= pc1 - acceptable_lower_pc1 :
            new_discount = 1 - (pc1 + pp) / (rrp)
            return new_discount
        return d

    df["optimum_discount"] = df.apply(lambda x: positive_pc1_discount_calculator(x["rrp_price"],
                                                                                x["purchase_price"],
                                                                                x["optimum_discount"]), axis = 1)

    df = pd.merge(left = df ,
            right = pc1[['Tracking Type Desc', 'Category Leaf Id', 'PC1PerUnit']] ,
            how = "left",
            left_on = ["tracking_type","leafcat_id"],
            right_on = ["Tracking Type Desc","Category Leaf Id"]) 
        
    df["optimum_discount"] = df.apply(lambda x: leafcat_pc1_discount_calculator(x["rrp_price"],
                                                                                x["purchase_price"],
                                                                                x["optimum_discount"],
                                                                                x["PC1PerUnit"]), axis = 1)

    df["optimum_discount"] = df["optimum_discount"].apply(lambda x: max(0, x))

    df.drop(columns = ["PC1PerUnit","Category Leaf Id", "Tracking Type Desc"], inplace = True)

    df["optimum_discount"] = df["optimum_discount"].round(3)

    df['suggested_price'] = (df['rrp_price'] * (1 - df['optimum_discount']))
    df['suggested_price'].dropna(inplace = True)
    df['suggested_price'] = df['suggested_price'].astype('int64')
    df['suggested_pc1perc_last_pp'] = ((((1 - df['optimum_discount']) * df['rrp_price']) - df['purchase_price']) / ((1 - df['optimum_discount']) * df['rrp_price'])).round(2)
    df['suggested_pc1perc_avg_pp'] = ((((1 - df['optimum_discount']) * df['rrp_price']) - df['average_purchase_price']) / ((1 - df['optimum_discount']) * df['rrp_price'])).round(2)


    return df