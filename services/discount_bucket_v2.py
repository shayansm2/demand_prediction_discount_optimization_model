import pandas as pd
import numpy as np

def discount_bucket(num_of_discount_clusters: 'int',
                    h_sales : 'Table',
                    start_end_data_discount_bucket: 'tuple' ) -> 'Table':
    """
    |--------------------------------------------------------------------------
    |   TASK:Discount clustering
    | It clusters the discount in leaf category level per discount type (promotion/incredible)
    | based on ASP, IS, and PC1 with considering different weights for each parameter
    |--------------------------------------------------------------------------
    |   INPUTS:
    | num_of_discount_clusters: number of clusters
    | h_sales: historical sales data
    | leaf_weights: weights of leaf categories
    | start_end_data_discount_bucket: start date and end date of using historical data 
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: table of discount buckets in leafcat_discounttype level
    |--------------------------------------------------------------------------
    
    """
    start_date = start_end_data_discount_bucket[0]
    end_date = start_end_data_discount_bucket[1]
    h_sales = h_sales.astype({"Date":"int32"})
    h_sales = h_sales[(h_sales['Date'] <= end_date)&(h_sales['Date'] >= start_date)]
    h_sales['discount%'] = h_sales['Item Discount Net'] / (h_sales['Item Discount Net'] + h_sales['NMV 1'])

    def discount_type_label(promotion, incredible):
        if promotion == 1 or promotion == "1" :
            return "promotion"
        elif incredible == 1 or incredible == "1":
            return "incredible"
        return "none"

    h_sales.rename(columns = {"Gross Margin Ratio Of NMV":"PC1%" , "# Net Items":"Net_Items" , "Leaf Cat":"Leaf_category"}, inplace = True)
    h_sales['discount_type'] = h_sales.apply(lambda x: discount_type_label(x['Promotion Flag'] , x['Incredible Flag']),  axis=1)
    h_sales['ASP'] = h_sales['NMV 1'] / h_sales['Net_Items']

    h_sales.replace([np.inf, -np.inf], np.nan, inplace = True)
    h_sales.dropna(inplace = True)
    leafcat_list = pd.Series.tolist(h_sales['Leaf_category'].drop_duplicates())

    df_final = pd.DataFrame(columns=["index", "min", "max", "Leaf_category", "discount_type"])

    for leafcat in leafcat_list:

        df_leafcat = h_sales[(h_sales["Leaf_category"] == leafcat)]
        discount_type_list = list(df_leafcat["discount_type"].unique())
        for discount_type in discount_type_list:

            df = df_leafcat[(df_leafcat["discount_type"] == discount_type)]

            ##### new #####
            if len(df) < 10:
                continue
            ##### new #####

            buckets = pd.DataFrame(
                                pd.qcut(df["discount%"],  
                                num_of_discount_clusters, 
                                duplicates = "drop"
                            ).unique(), columns = ["bucket_range"])
            buckets["min"] = buckets["bucket_range"].apply(lambda a: max(0,a.left))
            buckets["max"] = buckets["bucket_range"].apply(lambda a: a.right)
            buckets.sort_values("min", inplace= True, ignore_index=True)
            buckets.reset_index(inplace = True)
            buckets["Leaf_category"] = leafcat
            buckets["discount_type"] = discount_type

            df_final = df_final.append(buckets)
            df_final.dropna(inplace = True)

    return df_final