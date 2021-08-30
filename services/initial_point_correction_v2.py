import pandas as pd
import numpy as np

def initial_point_correction(df_main: 'table',
                             df_current: 'table') -> 'Table':
    """
    |--------------------------------------------------------------------------
    |   TASK: correcting initial point 
    | it will get the current initial point (weekly)
    | and monthly data and integrate it (use the weekly data
    | if not available use the monthly data)
    |--------------------------------------------------------------------------
    |   INPUTS:
    |       df_main: inital point monthly
    |       df_current: initial point weekly 
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    |       inital point corrected (use weekly, if not available monthly)
    |--------------------------------------------------------------------------
    
    """ 
    df_current_copy = df_current[['day_flag', 'DKP',"DKPC" , "tracking_type", 'discount_type', 'initial_item_sold']]
    df_current_copy.rename(columns = {"initial_item_sold":"initial_item_sold_new"}, inplace = True)

    df = pd.merge(left= df_main, 
                right = df_current_copy , 
                how = "outer", 
                on = ['day_flag', 'DKP', 'discount_type',"DKPC" , "tracking_type"])
    
    def initial_item_sold_correction(main, correction):

        beta = 0.8
        if np.isnan(correction) :
            return main
        if np.isnan(main) :
            return correction
        return (beta * main) + ((1-beta) * correction)

    df["initial_item_sold"] = df.apply(lambda x: initial_item_sold_correction(x["initial_item_sold"], 
                                                                            x["initial_item_sold_new"]), axis = 1)

    df.drop(columns = "initial_item_sold_new", inplace = True)
    return df