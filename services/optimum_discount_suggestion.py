#%%
import pandas as pd
import numpy as np
#%%

def optimum_discount_suggestion(asrt : 'table' ,
                                ffactor : 'table',
                                initial_points : 'table') -> 'table':
    """
    |--------------------------------------------------------------------------
    | Optimum Discount Suggestion: 
    |--------------------------------------------------------------------------
    |   TASK of Optimum Discount Suggestion: 
    | suggest a discount amount and a item sold predisction which will maximize
    | the total PC1 value
    |--------------------------------------------------------------------------
    |   INPUTS: 
    |		Assortment data
    |       f_factor table
    |       initla_point table
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    |       a table with the optimum discount value 
    |--------------------------------------------------------------------------
    """

    def warehouse_flag(id):
        if (id == "29") or (id == 29):
            return "Shad Abad"
        return "Danesh"
    
    asrt["tracking_type"] = asrt["warehouse_id"].apply(lambda x: warehouse_flag(x))
    asrt.drop(columns = "warehouse_id", inplace = True)

    day_flag_values = list(ffactor["day_flag"].unique())
    day_flag = pd.DataFrame({'key':[1]*len(day_flag_values), 'day_flag':day_flag_values})

    discount_type_values = list(ffactor["discount_type"].unique())
    discount_type = pd.DataFrame({'key':[1]*len(discount_type_values), 'discount_type':discount_type_values})

    asrt['key'] = 1
    asrt = pd.merge(left = asrt, right = day_flag, on = "key")
    asrt = pd.merge(left = asrt, right = discount_type, on = "key")
    asrt.drop(columns = ["key"], inplace = True)

    df = pd.merge(left = asrt, right = ffactor, how = "left", 
                  left_on = ["brand", "day_flag", "discount_type", "leaf_category", "tracking_type"] , 
                  right_on= ["Brand", "day_flag", "discount_type", "Leaf_category", "tracking_type"] )
    df.drop(columns = ["Leaf_category", "Brand"], inplace = True)

    df = pd.merge(left = df, right = initial_points, how = "inner", 
                  on = ["day_flag", "DKP", "discount_type", "DKPC", "tracking_type"] )
    df.drop(columns = ["day", "NMV 1", "Item Discount Net"], inplace = True)

    ffactor_per_leafcat = ffactor.groupby(by = ["Leaf_category",
                                                "discount_type",
                                                "tracking_type", 
                                                "day_flag"]).mean().reset_index()

    ffactor_per_leafcat.rename(columns = {"f_factor":"f_factor_per_leafcat"}, inplace = True)

    df = pd.merge(left = df, right = ffactor_per_leafcat, how = "left", 
                  left_on = ["day_flag", "discount_type", "leaf_category", "tracking_type" ] , 
                  right_on= ["day_flag", "discount_type", "Leaf_category", "tracking_type" ] )
    df.drop(columns = ["Leaf_category"], inplace = True)

    df["f_factor"] = df.apply(lambda x: (x["f_factor_per_leafcat"] if np.isnan(x["f_factor"]) else x["f_factor_per_leafcat"]), axis = 1)
    df.drop(columns = ["f_factor_per_leafcat"], inplace = True) 

    def discount_calculator_formula(pp, rrp, f, d0,  v = 0.09):

        if f <= 1:
            return 0
        if np.isnan(f):
            return d0
        discount_formula = 1 - ((pp*f)/((1-v)*rrp*(f-1)))
        if discount_formula < 0:
            return 0
        return discount_formula
    
    def net_item_predictor(d0, is0, optimum_discout, f):

        try:
            return is0 * (((1-d0)/(1-optimum_discout)) ** f)
        except:
            return is0
    
    def discount_calculator_formula_after_stock(d0, is0, stock, f, optimum_discout):
        try:
            return max( 1 - (1 - d0) * (np.power((is0 / stock) , 1/f)) , 0)
        except:
            return optimum_discout
    
    # def positive_pc1_discount_calculator(rrp, pp, d):
    #     discount_threshold = 1 - (pp / rrp)
    #     return min(d, discount_threshold)

    df["discount_before_stock_check"] = df.apply(lambda x:discount_calculator_formula(  x["purchase_price"],
                                                                                        x["rrp_price"],
                                                                                        x["f_factor"],
                                                                                        x["initial_discount_percent"]), axis = 1)

    df["item_sold_prediction"] = df.apply(lambda x: net_item_predictor( x["initial_discount_percent"],
                                                                        x["initial_item_sold_hourly_stock"],
                                                                        x["discount_before_stock_check"],
                                                                        x["f_factor"]), axis = 1)

    df["optimum_discount"] = df.apply(lambda x: discount_calculator_formula_after_stock(x["initial_discount_percent"],
                                                                                        x["initial_item_sold_hourly_stock"],
                                                                                        x["stock"],
                                                                                        x["f_factor"],
                                                                                        x["discount_before_stock_check"]), axis = 1)


    # df["optimum_discount"] = df.apply(lambda x: positive_pc1_discount_calculator(x["rrp_price"],
    #                                                                             x["purchase_price"],
    #                                                                             x["optimum_discount"]), axis = 1)

    
    # df["optimum_discount"] = df["optimum_discount"].round(3)

    return df