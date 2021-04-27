#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 17:33:27 2021

@author: Gustavo
"""

import pandas as pd
import productScraper as ps
import json
import RDConnection as rd
import os
from itertools import repeat, compress
import re
import time


def keyword_scrape_to_file():

    EXCEL_FILE = 'Taxonomy_Words_No_Duplicates.xlsx'
    
    
    df = pd.read_excel(EXCEL_FILE, sheet_name='Google', usecols = "A", engine='openpyxl').dropna().loc[:,'WORDS'].values
    with open('config.json', 'r') as json_file:
        config = json.load(json_file)['config']
    
    
    LAST_KEYWORD_INDEX = config['last index']
    THRESHOLD = config['threshold']
    PACE = config['pace']
    sleep_time = config['sleep']
    df_to_scrape = df[LAST_KEYWORD_INDEX : LAST_KEYWORD_INDEX + THRESHOLD]
    
    
    complete_list_of_keywords = []
    
    try:
        for i in range(0,len(df_to_scrape),PACE):
            list_of_keywords = ps.webscraping_keywords(df_to_scrape[i:i+PACE])
            print('over {}/{}'.format(int(i/PACE)+1, int(len(df_to_scrape)/PACE)))
            file_name = f'Keywords/JSON/JSS_List_of_keywords_from_{i+LAST_KEYWORD_INDEX:020b}_to_{i+LAST_KEYWORD_INDEX+PACE:020b}'
            ps.save_file(list_of_keywords, file_name)
            complete_list_of_keywords.extend(list_of_keywords)
            print(f'Sleeping for {int(sleep_time/3)} s')
            time.sleep(sleep_time/3)
            
    
    except:
        pass
    else:     
        config['last index'] += THRESHOLD 
        with open('config.json', 'w') as json_file:
            json.dump({"config": config}, json_file)   
    finally:
        complete_list_of_keywords = ps.remove_duplicates_from_list(complete_list_of_keywords)
        dataframe_keywords = ps.transform_keyword_to_pd(complete_list_of_keywords)
        file_name = "Keywords/CSV/JSS_List_of_keywords_from_{}_to_{}.csv".format(LAST_KEYWORD_INDEX, LAST_KEYWORD_INDEX+THRESHOLD)
        dataframe_keywords.to_csv(file_name, index=False, sep = '\t')

        
def product_scrape_to_file():
    try:
        database = rd.DataBaseConnection()
        list_of_keyword_names = database.get_products_to_scrape()
        list_of_keyword_names = [name[0] for name in list_of_keyword_names]
        end = len(list_of_keyword_names)
        with open('config.json', 'r') as json_file:
            config = json.load(json_file)['config']
        pace = config['product pace']
        sleep_time = config['sleep']
    except:
        pass
    else:
        complete_list_of_products = []
        print('Starting to scrape products')
        for i in range(0, end, pace):
            petit_list = list_of_keyword_names[i:i+pace]
            prods_list = ps.webscraping_products(petit_list)
            complete_list_of_products.extend(prods_list)
            file_name = f'Products/JSON/JSS_List_of_products_from_{i:020b}_to_{i+pace:020b}'
            ps.save_file(prods_list, file_name)
            print(f'Finished task {int(i/pace)+1}/{int(end/pace)}')
            print(f'Sleeping for {sleep_time} s')
            time.sleep(sleep_time)
        data_frame = ps.transform_product_database_to_pd(complete_list_of_products)
        try:
            data_frame = data_frame.drop_duplicates(subset=['id'])
            print('Dropped duplicates')
        except:
            print('oops! Could not drop duplicates')
        finally:
            csv_file_name = "Products/CSV/JSS_List_of_products_from_0_to_{}.csv".format(i+pace)
            data_frame.to_csv(csv_file_name, index=False, sep = '\t')
            


def json_to_csv(type_data):
    with open('config.json', 'r') as json_file:
        config = json.load(json_file)['config']
    THRESHOLD = config['threshold']
    PACE = config['pace']
    batch_of = int(THRESHOLD/PACE)
    print(batch_of)
    if type_data.lower() == 'keyword':
        dir_name = 'Keywords/'
    elif type_data.lower() == 'product':
        dir_name = 'Products/'
    path = dir_name + 'JSON/'
    list_of_json = os.listdir(path)
    list_of_json = [''.join([path, x]) for x in filter(lambda a: a.startswith('JSS'), list_of_json)]
    print(len(list_of_json))
    for i in range(0,len(list_of_json), batch_of):
        files_to_combine = []
        for file_name in list_of_json[i:i+batch_of]:
            with open(file_name, 'r') as json_file: 
                files_to_combine.extend(json.load(json_file))
        if type_data.lower() == 'keyword':
            data_frame = ps.transform_keyword_to_pd(files_to_combine)
        elif type_data.lower() == 'product':
            data_frame = ps.transform_product_database_to_pd(files_to_combine)
        print('Finished transforming to data frame')
        try:
            data_frame = data_frame.drop_duplicates(subset=['id'])
            print('Dropped duplicates')
        except:
            print('oops! Could not drop duplicates')
        list_index = list(map(re.findall, repeat(r'\d+'), list_of_json[i:i+batch_of]))
        name = f'JSS_List_of_{type_data.lower()}s_from_{int(list_index[0][0],2)}_to_{int(list_index[-1][1],2)}.csv'
        csv_file_name = dir_name + 'CSV/' + name
        data_frame.to_csv(csv_file_name, index=False, sep = '\t')
        print(f'Finished task {int(i/batch_of)+1}/{int(len(list_of_json)/batch_of)}')
        time.sleep(1)
    

def save_into_database(type_data):
    switcher = {'keyword': 'keyword_table',
                'product': 'products_database_table'}
    try:
        table_name = switcher.get(type_data, 'Invalid data!')
        if table_name == 'Invalid data!':
            raise Exception('Data type needs to be "keyword" or "product"')    
    except Exception as err:
        print(err)
        pass
    else:
        if type_data.lower() == 'keyword':
            dir_name = 'Keywords/'
        elif type_data.lower() == 'product':
            dir_name = 'Products/'
        try:
            dir_name = dir_name + 'CSV/'
            list_of_csv = os.listdir(dir_name)
            list_of_csv = [''.join([dir_name, x]) for x in filter(lambda a: a.startswith('JSS'), list_of_csv)]
            if not list_of_csv or not isinstance(list_of_csv, list):
                raise Exception('Ops! Could not get CSV files')
        except Exception as inst:
            print(inst)
            pass
        else:
            database = rd.DataBaseConnection()
            boolean_tasks = list(map(database.insert_new_record_from_csv, repeat(table_name), list_of_csv))
            database.close() 
            print(boolean_tasks)
            try:
                list(map(os.remove, list(compress(list_of_csv, boolean_tasks)))) 
            except OSError as err:
                print(err)
                pass
            
    
    
if __name__ == "__main__":
    keyword_scrape_to_file()
    time.sleep(600)
    product_scrape_to_file()
    list(map(save_into_database, ['keyword', 'product']))
    
   