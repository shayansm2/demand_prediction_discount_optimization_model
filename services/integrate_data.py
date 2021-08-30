import pandas as pd
import numpy as np
import jdatetime

def integrate_data(h_sales : 'table',
                    discount_bucket_result: 'table', 
                    marketing_calendar : 'table',
                    time_period_flag : 'dictionary',
                    promotion_discount_anomaly : 'float' = 0.4 ) -> 'table':
    """
    |--------------------------------------------------------------------------
    | Integrate Data (use for sales aggregation and initial point indicator)
    |--------------------------------------------------------------------------
    |   TASK of Integrate Data:
    | it will integrate the result table from discount_bucket method
    | with marketing promotion calender and sales data
    |--------------------------------------------------------------------------
    |   INPUTS:
    | h_sales: sales data
    | discount_bucket_result: the output table from discount_bucket
    | marketing_calendar: calander of marketing (source Gdoc) for campaigns, 
    | holidays, etc.
    | promotion_discount_anomaly: the maximum acceptable discount for promotions
    | time_period_flag: a dictionary which maps months to time periods 
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: integrated data (sales + marketing + discount bucket)
    |--------------------------------------------------------------------------
    """
    h_sales['discount%'] = h_sales['Item Discount Net'] / (h_sales['Item Discount Net'] + h_sales['NMV 1'])
    h_sales['discount%'].fillna(0, inplace = True)
    h_sales = h_sales.astype({"Date":"int32"})
    h_sales.rename(columns = {"Leaf Cat":"Leaf_category"}, inplace = True)

    def discount_type_label(promotion, incredible):
        if promotion == 1 or promotion == "1" :
            return "promotion"
        elif incredible == 1 or incredible == "1":
            return "incredible"
        return "none"

    h_sales.rename(columns = {"Gross Margin Ratio Of NMV":"PC1%" , "# Net Items":"Net_Items" , "Leaf Cat":"Leaf_category"}, inplace = True)
    h_sales.replace([np.inf, -np.inf], np.nan, inplace = True)
    h_sales.dropna(inplace = True)

    h_sales['discount_type'] = h_sales.apply(lambda x: discount_type_label(x['Promotion Flag'] , x['Incredible Flag']),  axis=1)
    h_sales['ASP'] = h_sales['NMV 1'] / h_sales['Net_Items']

    
    df = pd.merge(left=h_sales, right=marketing_calendar[['DateFA','Event Code','Holiday']], how='left', left_on = 'Date', right_on='DateFA')

    def day_flag_indicator(day_number_of_week,event_code, holiday):

        if event_code in ['PD','CPA1','CPA2','MC1','MC2','FS']:
            if event_code == 'FS':
                return 'FS'
            else:
                return 'Campaign'
        else:
            if holiday == 0.2:
                return "Holiday"
            elif ((holiday == 0.1) or 
                    (day_number_of_week == 6) or 
                    (day_number_of_week == 7) or
                    (day_number_of_week == "6") or 
                    (day_number_of_week == "7")) :
                return "Weekend"
            else:
                return "Normal day"

    df['day_flag'] = df.apply(lambda x: day_flag_indicator(x['Day Number Of Week'] , x['Event Code'], x['Holiday']),  axis=1)
    
    def discount_bucket_indicator(leaf_cat, discount_type, discount):

        query = discount_bucket_result[(discount_bucket_result['Leaf_category'] == leaf_cat)&(discount_bucket_result['discount_type'] == discount_type)]
        result = query[(query['min']<=discount)&(query['max']>discount)]['index']
        if len(result) == 1:
            return int(result)
        return 0

    df['discount_bucket'] = df.apply(lambda x: discount_bucket_indicator(x['Leaf_category'] ,x['discount_type'] ,x['discount%']), axis = 1)
    df = pd.merge(left = df, right = discount_bucket_result.reset_index()[['index','min','max','Leaf_category','discount_type']], how = "left", left_on = ["Leaf_category","discount_type","discount_bucket"], right_on = ["Leaf_category","discount_type","index"])
    anomaly_df_index = df[(df['discount_type'] == "promotion")&(df['discount%'] > promotion_discount_anomaly)].index
    df.drop(index = anomaly_df_index, inplace = True)

    month_number_dict = {"Farvardin":1,"Ordibehesht":2,"Khordad":3,
                        "Tir":4,"Mordad":5,"Shahrivar":6,
                        "Mehr":7,"Aaban":8,"Aazar":9,
                        "Dey":10,"Bahman":11,"Esfand":12}

    df['month_number'] = df["Month"].apply(lambda x: month_number_dict[x])
    df['year_month'] = df['Year'].apply(lambda x: str(x)) + "-" + df["month_number"].apply(lambda x: str(x))
    # df['time_period'] = df['year_month'].apply(lambda x: time_period_flag[x])
    df.rename(columns = {"Tracking Type Desc":"tracking_type"}, inplace = True)

    return df