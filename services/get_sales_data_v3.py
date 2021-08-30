import pandas as pd
import ssas_connection as sc
import db_connection as dc
from os import path

def get_monthly_sales_data(category : 'string',
                           execute_mdx : 'boolian' = False ,    
                           months : 'touple' = (5,12) ) -> 'table':    
    """
    |--------------------------------------------------------------------------
    | Get Sales Data (from Cube)
    |--------------------------------------------------------------------------
    |   TASK of get_sales_data:
    | get data from cube
    |--------------------------------------------------------------------------
    |   INPUTS:
    |  category: get the sales data for this category
    | months (from , to) : from month - to month  
    | execute_mdx : if TRUE execute the MDX and update the sales file
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: returnes sales table
    |--------------------------------------------------------------------------
    """

    file_directory = r"data\{}\monthly_sales_data.csv".format(category)

    mdx_query= '''SELECT { 
                    [Measures].[NMV 1],
                    [Measures].[# Net Items],
                    [Measures].[Item Discount Net], 
                    [Measures].[NMV 3],
                    [Measures].[Gross Margin 3], 
                    [Measures].[COGS], 
                    [Measures].[Gross Margin Ratio Of NMV]
                    } ON COLUMNS, 
                    NON EMPTY { (
                    [Dim Product].[Product Id].[Product Id].ALLMEMBERS *
                    [Dim Product].[Product Item Id].[Product Item Id].ALLMEMBERS *
                    [Dim Product].[Business Type].[Business Type].&[Retail] *
					[Dim Tracking Type].[Tracking Type Desc].[Tracking Type Desc].ALLMEMBERS *
                    [Dim Product].[Brand Name En].[Brand Name En].ALLMEMBERS *
                    [Dim Date].[YSMDate].[Persian Date].ALLMEMBERS * 
                    [Dim Date].[Day Number Of Week].[Day Number Of Week].ALLMEMBERS * 
                    [Dim Date].[Persian Day Number Of Month].[Persian Day Number Of Month].ALLMEMBERS * 
                    [Dim Promotion Flag].[Is Promotion].[Is Promotion].ALLMEMBERS * 
                    [Dim Incredible].[Incredible Flag].[Incredible Flag].ALLMEMBERS * 
                    [Dim Product].[Category Leaf Name].[Category Leaf Name].ALLMEMBERS ) } ON ROWS 
                    FROM ( SELECT ( { [Dim Digi Status].[Digi Status].&[40], [Dim Digi Status].[Digi Status].&[50], [Dim Digi Status].[Digi Status].&[70] } ) ON COLUMNS 
                    FROM ( SELECT ( { [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''] } ) ON COLUMNS  
                    FROM ( SELECT ( { [Dim Date].[YMD].[Persian Year].&[1400] } , 
                    [Dim Date].[Persian Month Number].[Persian Month Number].&[''' + str(months[0]) + '''] :
                    [Dim Date].[Persian Month Number].[Persian Month Number].&[''' + str(months[1]) + '''] ) ON COLUMNS 
                    FROM [DW Digi Kala])))
                    WHERE ( [Dim Date].[YMD].CurrentMember, 
                    [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''], 
                    [Dim Digi Status].[Digi Status].CurrentMember )
                    '''
    
    if not path.exists(file_directory):
        execute_mdx = True

    if execute_mdx:
        conn = sc.set_conn_string()
        df_cube = sc.get_MDX(connection_string=conn,mdx_string=mdx_query)

        change_headers = {"Product Id":"DKP",
                        "Product Item Id" : "DKPC",
                        "Brand Name En":"Brand",
                        "Persian Year":"Year",
                        "Perisan Season Name En":"Season",
                        "Persian Month Name En":"Month",
                        "Persian Date":"Date",
                        "Persian Day Number Of Month":"Day Number Of Month",
                        "Is Promotion":"Promotion Flag",
                        "Category Leaf Name":"Leaf Cat"}
        df_cube.rename(columns = change_headers, inplace = True)

        df_cube.to_csv(file_directory,index = False)
    else:
        df_cube = pd.read_csv(file_directory)
    
    return df_cube





def get_weekly_sales_data(category : 'string',
                          execute_mdx : 'boolian' = False ,    
                          week_lags : 'touple' = (0,1) ) -> 'table':
    """
    |--------------------------------------------------------------------------
    | Get Sales Data (from Cube)
    |--------------------------------------------------------------------------
    |   TASK of get_sales_data:
    | get data from cube
    |--------------------------------------------------------------------------
    |   INPUTS:
    | category: get the sales data for this category
    | week_lags (from , to) : from month - to month  
    | execute_mdx : if TRUE execute the MDX and update the sales file
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: returnes sales table
    |--------------------------------------------------------------------------
    """

    file_directory = r"data\{}\weekly_sales_data.csv".format(category)

    mdx_query= '''SELECT { 
                    [Measures].[NMV 1],
                    [Measures].[# Net Items],
                    [Measures].[Item Discount Net], 
                    [Measures].[NMV 3],
                    [Measures].[Gross Margin 3], 
                    [Measures].[COGS], 
                    [Measures].[Gross Margin Ratio Of NMV]
                    } ON COLUMNS, 
                    NON EMPTY { (
                    [Dim Product].[Product Id].[Product Id].ALLMEMBERS *
                    [Dim Product].[Product Item Id].[Product Item Id].ALLMEMBERS *
                    [Dim Product].[Business Type].[Business Type].&[Retail] *
					[Dim Tracking Type].[Tracking Type Desc].[Tracking Type Desc].ALLMEMBERS *
                    [Dim Product].[Brand Name En].[Brand Name En].ALLMEMBERS *
                    [Dim Date].[YSMDate].[Persian Date].ALLMEMBERS * 
                    [Dim Date].[Day Number Of Week].[Day Number Of Week].ALLMEMBERS * 
                    [Dim Date].[Persian Day Number Of Month].[Persian Day Number Of Month].ALLMEMBERS * 
                    [Dim Promotion Flag].[Is Promotion].[Is Promotion].ALLMEMBERS * 
                    [Dim Incredible].[Incredible Flag].[Incredible Flag].ALLMEMBERS * 
                    [Dim Product].[Category Leaf Name].[Category Leaf Name].ALLMEMBERS ) } ON ROWS 
                    FROM ( SELECT ( { [Dim Digi Status].[Digi Status].&[40], [Dim Digi Status].[Digi Status].&[50], [Dim Digi Status].[Digi Status].&[70] } ) ON COLUMNS 
                    FROM ( SELECT ( { [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''] } ) ON COLUMNS  
                    FROM ( SELECT ( { [Dim Date].[YMD].[Persian Year].&[1400] } , 
                    [Dim Date].[Week Lag].[Week Lag].&[''' + str(week_lags[0]) + ''']:
					[Dim Date].[Week Lag].[Week Lag].&[''' + str(week_lags[1]) + ''']
					) ON COLUMNS 
                    FROM [DW Digi Kala])))
                    WHERE ( [Dim Date].[YMD].CurrentMember, 
                    [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''], 
                    [Dim Digi Status].[Digi Status].CurrentMember )
                    '''
    
    if not path.exists(file_directory):
        execute_mdx = True

    if execute_mdx:
        conn = sc.set_conn_string()
        df_cube = sc.get_MDX(connection_string=conn,mdx_string=mdx_query)

        change_headers = {"Product Id":"DKP",
                        "Product Item Id" : "DKPC",
                        "Brand Name En":"Brand",
                        "Persian Year":"Year",
                        "Perisan Season Name En":"Season",
                        "Persian Month Name En":"Month",
                        "Persian Date":"Date",
                        "Persian Day Number Of Month":"Day Number Of Month",
                        "Is Promotion":"Promotion Flag",
                        "Category Leaf Name":"Leaf Cat"}
        df_cube.rename(columns = change_headers, inplace = True)

        df_cube.to_csv(file_directory,index = False)
    else:
        df_cube = pd.read_csv(file_directory)
    
    return df_cube




def get_last_30_days_margin(category : 'string') -> 'table':
    """
    |--------------------------------------------------------------------------
    | 
    |--------------------------------------------------------------------------
    |   
    |--------------------------------------------------------------------------
    |   
    |--------------------------------------------------------------------------
    |   
    |--------------------------------------------------------------------------
    """

    mdx_query= '''SELECT {[Measures].[Gross Margin 3]} ON COLUMNS, 
                    NON EMPTY { (
					[Dim Product].[Product Item Id].[Product Item Id].ALLMEMBERS *
                    [Dim Product].[Product Id].[Product Id].ALLMEMBERS *
                    [Dim Product].[Business Type].[Business Type].&[Retail] *
					[Dim Tracking Type].[Tracking Type Desc].[Tracking Type Desc].ALLMEMBERS *
					[Dim Product].[Category Leaf Id].[Category Leaf Id].ALLMEMBERS) } ON ROWS 
                    FROM ( SELECT ( { [Dim Digi Status].[Digi Status].&[40], [Dim Digi Status].[Digi Status].&[50], [Dim Digi Status].[Digi Status].&[70] } ) ON COLUMNS 
                    FROM ( SELECT ( { [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''] } ) ON COLUMNS  
                    FROM ( SELECT ( { [Dim Date].[YMD].[Persian Year].&[1400] }
					, [Dim Date].[Prev30 Days].[Prev30 Days].&[1]
					) ON COLUMNS 
                    FROM [DW Digi Kala])))
                    WHERE ( [Dim Date].[YMD].CurrentMember, 
                    [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''], 
                    [Dim Digi Status].[Digi Status].CurrentMember )
                    '''
    
    conn = sc.set_conn_string()
    df_cube = sc.get_MDX(connection_string=conn,mdx_string=mdx_query)

    change_headers = {"Product Id":"DKP",
                    "Product Item Id" : "DKPC"}
    df_cube.rename(columns = change_headers, inplace = True)

    return df_cube



    
def get_last_30_days_pc1PerUnit_per_leafcat(category : 'string') -> 'table':
    """
    |--------------------------------------------------------------------------
    | 
    |--------------------------------------------------------------------------
    |   
    |--------------------------------------------------------------------------
    |   
    |--------------------------------------------------------------------------
    |   
    |--------------------------------------------------------------------------
    """

    mdx_query= '''
    with member [PC1PerUnit] as [Measures].[Gross Margin 3] / [Measures].[# Net Items]
    SELECT [PC1PerUnit] ON COLUMNS, 
    NON EMPTY { (
    [Dim Product].[Business Type].[Business Type].&[Retail] *
	[Dim Tracking Type].[Tracking Type Desc].[Tracking Type Desc].ALLMEMBERS *
    [Dim Product].[Category Leaf Name].[Category Leaf Name].ALLMEMBERS *
    [Dim Product].[Category Leaf Id].[Category Leaf Id].ALLMEMBERS) } ON ROWS 
    FROM ( SELECT ( { [Dim Digi Status].[Digi Status].&[40], [Dim Digi Status].[Digi Status].&[50], [Dim Digi Status].[Digi Status].&[70] } ) ON COLUMNS 
    FROM ( SELECT ( { [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''] } ) ON COLUMNS  
    FROM ( SELECT ( { [Dim Date].[YMD].[Persian Year].&[1400] }
    , [Dim Date].[Prev30 Days].[Prev30 Days].&[1]
    ) ON COLUMNS 
    FROM [DW Digi Kala])))
    WHERE ( [Dim Date].[YMD].CurrentMember, 
    [Dim Product].[Category Leve1 Name Fa].[Category Leve1 Name Fa].&[''' + str(category) + '''], 
    [Dim Digi Status].[Digi Status].CurrentMember )'''
    
    conn = sc.set_conn_string()
    df_cube = sc.get_MDX(connection_string=conn,mdx_string=mdx_query)

    return df_cube