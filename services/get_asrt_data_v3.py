import pandas as pd
from os import path
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector

def db_connect():

    server = "server_ip"
    username = "user_name"
    dbname = "database_name"
    passw = "password"
    prt = 'port'
    engine = create_engine('mysql://'+str(username)+':'+str(passw) +
                           '@'+str(server) + ":" + str(prt) +'/'+str(dbname)+'?charset=utf8mb4')
    conn = engine.connect()
    connection = mysql.connector.connect(host=server,
                             database=dbname,
                             user=username,
                             password=passw,
                             use_pure=True,
                             port = prt,
                             auth_plugin='mysql_native_password')
    return conn, connection

def test_query(conn, connection, query):
    
    dfl = []
    pdf = pd.DataFrame()

    for chunks in pd.read_sql_query(sql = query, con=conn, chunksize = 10):
        # print('query is done')
        dfl.append(chunks)
        # print('Chunk is done')
    pdf = pd.concat(dfl, ignore_index=True)
    # print('concat is done')    
    return pdf
    # pdf.to_csv(path_or_buf = r'/home/hamid/serial.csv', index= False, encoding= 'utf-8')
    # print('to csv is done')

def get_asrt_data(category : 'string',
                    execute_sql : 'boolian' = False) -> 'table':
    """
    |--------------------------------------------------------------------------
    | Get ASRT Data (from DB)
    |--------------------------------------------------------------------------
    |   TASK of get_asrt_data:
    | get data from DB
    |--------------------------------------------------------------------------
    |   INPUTS:  
    | execute_sql : if TRUE execute the SQL and update the sales file
    | file directory : where to save the file
    |--------------------------------------------------------------------------
    |   OUTPUTS:
    | output: returnes ASRT table
    |--------------------------------------------------------------------------
    """

    file_directory : 'string' = r"data\{}\asrt_data.csv".format(category)

    query = '''     
    SELECT
        pv.id AS DKPC,
        p.id AS DKP,
        p.title_fa AS 'Product_title',
        p.title_en AS 'Product_title_en',
        sc.title_fa AS 'Cat',
        c.id AS 'leafcat_id',
        c.title_fa AS 'leaf_category',
        c.title_en AS 'Leaf_category_en',
        b.name_en AS 'brand',
        v.rrp_price,
        v.selling_price,
        (v.selling_price - v.rrp_price) AS 'current_discount',
        IF(v.tags = 'incredible_offer', 1, 0) AS 'amazing_right_now',
        w.id as 'warehouse_id',
        w.title_fa as 'warehouse',
    IF
        (
            pv.warehouse_stock + pv.warehouse_supply_stock - pv.seller_reserved_stock >= 0,
            pv.warehouse_stock + pv.warehouse_supply_stock - pv.seller_reserved_stock,
            0 
        ) AS 'stock',
        pv.last_purchase_fixed_price AS 'purchase_price' ,
        pv.avg_purchase_price AS 'average_purchase_price'
    FROM
        supply_categories sc
        JOIN supply_category_tree sct ON sc.id = sct.parent_id 
        AND sc.title_fa = '''+ "'" + str(category) + "'" +'''
        JOIN categories c ON sct.category_id = c.supply_category_id
        JOIN products p ON c.id = p.category_id
        JOIN product_variants pv ON p.id = pv.product_id 
        AND p.moderation_status IN ( 'approved' ) 
        AND pv.selling_stock > 0 
        AND p.product_type = 'product' 
        AND p.`status` = 'marketable' 
        AND p.active = 1 
        AND pv.active = 1
        JOIN brands b ON b.id = p.brand_id
        JOIN variant_prices v ON v.id = COALESCE ( pv.promotion_price_id, pv.default_price_id ) 
        JOIN warehouse_stocks ws ON ws.variant_id = pv.id AND ws.stocks > 0 
        JOIN warehouses as w on w.id = ws.warehouse_id
    WHERE
        pv.last_purchase_fixed_price IS NOT NULL 
        AND pv.warehouse_stock + pv.warehouse_supply_stock - pv.seller_reserved_stock > 0  '''
    
    if not path.exists(file_directory):
        execute_sql = True

    if execute_sql:
        conn, connection = db_connect()
        df = test_query(conn, connection, query)
        conn.close()

        df.to_csv(file_directory,index = False)
    else:
        df = pd.read_csv(file_directory)
    
    return df
