import pandas as pd
import ssas_connection as sc
from os import path

def get_hourly_sales_weights(category : 'string',
                            execute_mdx : 'boolian' = False ) -> 'table':    
    """
    |--------------------------------------------------------------------------
    | Get Houly Sales Weights Data (from Cube)
    |--------------------------------------------------------------------------
    |   TASK of get_hourly_sales_weights:
    | get data from cube
    |--------------------------------------------------------------------------
    |   INPUTS:
    |  category: get the sales data for this category  
    | execute_mdx : if TRUE execute the MDX and update the sales file
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: returnes hourly sales wights table
    |--------------------------------------------------------------------------
    """

    file_directory = r"data\{}\hourly_sales_weights.csv".format(category)

    mdx_query= '''
                select [Measures].[Net Item Fcast] on 0,
                [Dim Time].[Hour].[Hour].members
                on 1
                from [DW Digi Kala]
                where (
                {[Dim Digi Status].[Digi Status].[Digi Status].&[40]
                ,[Dim Digi Status].[Digi Status].[Digi Status].&[50]
                ,[Dim Digi Status].[Digi Status].[Digi Status].&[70]}
                ,[Dim Date].[Prev30 Days].[Prev30 Days].&[1]
                ,[Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''])
                '''
    
    if not path.exists(file_directory):
        execute_mdx = True

    if execute_mdx:

        conn = sc.set_conn_string()
        df_cube = sc.get_MDX(connection_string=conn,mdx_string=mdx_query)
        total_item_sold = df_cube["Net Item Fcast"].sum()
        df_cube["Net Item Fcast"] = df_cube["Net Item Fcast"] / total_item_sold
        df_cube.rename(columns = {"Net Item Fcast":"weight"}, inplace = True)
        df_cube.to_csv(file_directory,index = False)

    else:
        df_cube = pd.read_csv(file_directory)
    
    return df_cube