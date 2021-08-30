import pandas as pd
import numpy as np


def add_abc_analysis_flag(df_final: 'table',
                          df_gm: 'table') -> 'table':
    """
    |--------------------------------------------------------------------------
    | Add ABC analysis flag
    |--------------------------------------------------------------------------
    |   TASK of add_abc_analysis_flag:
    |   
    |       |volume |value  |bucket |flag              |
    |       |=======|=======|=======|==================|
    |       |15%-20%|75%-80%|   A   | high importance  |
    |       |20%-25%|10%-15%|   B   | mid  importance  |
    |       |60%-65%| 5%-10%|   C   | low  importance  |
    |--------------------------------------------------------------------------
    |   INPUTS:
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    |--------------------------------------------------------------------------
    """
    df_gm["absolute_gm"] = np.abs(df_gm["Gross Margin 3"])
    df_gm.sort_values("Gross Margin 3", ascending=False, inplace=True)
    total_gm = sum(df_gm["absolute_gm"])
    df_gm["absolute_gm_ratio"] = df_gm["absolute_gm"] / total_gm
    df_gm["cumsum"] = np.cumsum(df_gm["absolute_gm_ratio"])

    def flag(cumsum):
        if cumsum <= 0.75:
            return "high importance"
        if cumsum <= 0.9:
            return "mid importance"
        return "low importance"

    df_gm["product_flag"] = df_gm["cumsum"].apply(lambda x: flag(x))
    df_gm = df_gm[["DKPC", "product_flag"]]

    df_final = pd.merge(left=df_final, right=df_gm, how="left", on="DKPC")
    df_final["product_flag"].fillna("low importance", inplace=True)

    return df_final
