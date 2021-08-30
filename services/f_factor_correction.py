import pandas as pd
import numpy as np


def f_factor_correction(df_main: 'table',
                        df_correction: 'table') -> 'Table':
    """
    |--------------------------------------------------------------------------
    |   TASK: correcting f_factor 
    | it will get the current f factor (weekly)
    | and monthly data and integrate it (using exponential moving average
    |--------------------------------------------------------------------------
    |   INPUTS:
    |       df_main: f factor monthly
    |       df_correction: f factor weekly 
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    |       f factor corrected (use exponential moving average)
    |--------------------------------------------------------------------------
    
    """
    df = pd.merge(left=df_main,
                  right=df_correction,
                  on=['Brand', 'day_flag', 'Leaf_category', 'discount_type', 'tracking_type'],
                  how="outer")

    def f_factor_correction(main, correction):

        beta = 0.9
        if np.isnan(correction):
            return main
        if np.isnan(main):
            return correction
        return (beta * main) + ((1 - beta) * correction)

    df["f_factor"] = df.apply(lambda x: f_factor_correction(x["f_factor_x"], x["f_factor_y"]), axis=1)
    df.drop(columns=["f_factor_x", "f_factor_y"], inplace=True)
    return df

# %%
