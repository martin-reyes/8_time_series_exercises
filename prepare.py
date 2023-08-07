import os
import pandas as pd
import acquire as a


def prep_sales():
    
    # acquire data
    # if file doesn't exist, cache it
    filepath = 'data/store_data.csv'
    if not os.path.isfile(filepath):
        sql = '''SELECT *
                 FROM sales
                     JOIN items USING (item_id)
                     JOIN stores USING (store_id);'''
        sales = pd.read_sql(sql, acquire_utils.get_connection('tsa_item_demand'))
        sales.to_csv(filepath, index=False)
    
    sales = pd.read_csv(filepath)
    
    # convert date column to datetime object and set as the index
    sales['sale_date'] = sales['sale_date'].astype('datetime64')
    sales = sales.set_index('sale_date').sort_index()
    
    # create month and weekday column
    sales['month'] = sales.index.month
    sales['day_of_week'] = sales.index.weekday
    
    # create sales_total column
    sales['sales_total'] = sales['sale_amount'] * sales['item_price']
    
    return sales





def prep_ops_data(ops_data = a.acquire_ops_data()):
    
    # convert all columns to lowercase
    ops_data.columns = [col.lower() for col in ops_data.columns]
    
    # convert date to datetime index
    ops['date'] = ops['date'].astype('datetime64')
    ops = ops.set_index('date').sort_index()
    
    # add month and year column
    ops['month'] = ops.index.month
    ops['year'] = ops.index.year
    
    # impute 0 for missing values
    ops = ops.fillna(0)
    
    return ops