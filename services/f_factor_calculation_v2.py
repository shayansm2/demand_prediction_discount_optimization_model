import pandas as pd
import numpy as np


def f_factor_calculation(df: 'table',
                         min_acceptable_ffactor: 'number' = 1) -> 'table':
    """
    |--------------------------------------------------------------------------
    | F Factor calculation: Calculating final F factor in determined level based on historical data
    |--------------------------------------------------------------------------
    |   TASK of F Factor calculation: Calculating final F factor in determined level based on historical data
    |--------------------------------------------------------------------------
    |   INPUTS: df: Historical sales data in determined time period
    |		min_acceptable_ffactor: Acceptable F factor values threshold
    |--------------------------------------------------------------------------
    |   OUTPUTS: df_final: Final F factor in determined level (for example: leafcat_brand_discountType level)
    |--------------------------------------------------------------------------
    """
    # step one --> aggregate  time_period and get rid of it
    group = ['max', 'min', 'Brand', 'discount_bucket', 'Leaf_category', 'discount_type', 'day_flag', 'tracking_type']
    df_max_days = df.groupby(by=group).agg({'day_with_sales_in_cluster': 'max'}).reset_index()
    df = pd.merge(how="inner", left=df, right=df_max_days, on=group + ['day_with_sales_in_cluster'])
    df_max_potential = df.groupby(by=group).agg({'sales_potential': 'max'}).reset_index()
    df = pd.merge(how="inner", left=df, right=df_max_potential, on=group + ['sales_potential'])
    df = df.groupby(by=group).mean().reset_index()
    df.drop(index=df[df["discount_in_cluster"] == 1].index, inplace=True)

    df["x"] = df["discount_in_cluster"].apply(lambda d: np.log(1 / (1 - d)))
    df["y"] = df["sales_potential"].apply(lambda i: np.log(i))
    df["xy"] = df["x"] * df["y"]
    df["x**2"] = df["x"] * df["x"]

    group = ['Brand', 'Leaf_category', 'discount_type', 'day_flag', 'tracking_type']
    mse_measures = ["x", "y", "xy", "x**2"]

    final_df = df[group + mse_measures].groupby(by=group).sum().reset_index()
    final_df["f_factor"] = ((final_df["x"] * final_df["y"]) - final_df["xy"]) / (
                (final_df["x"] * final_df["x"]) - final_df["x**2"])
    final_df.drop(columns=["x", "y", "x**2", "xy"], inplace=True)
    final_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    final_df.dropna(inplace=True)

    return final_df
