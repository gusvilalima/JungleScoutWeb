#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 14:24:45 2021

@author: Gustavo
"""

import requests
import time
import re
import ast
import numpy as np
import pandas as pd
import asyncio
import concurrent.futures
import nest_asyncio
import os.path
import random
from collections import defaultdict
import ujson
import functools
import json



with open("configWeb.json", "r") as json_file:
    config = json.load(json_file)



SIGN_IN_URL = config['SIGN_IN_URL']
KEYWORD_URL = config['KEYWORD_URL']
SUPPLIERS_URL = config['SUPPLIERS_URL']
PRODUCT_DATABASE_URL = config['PRODUCT_DATABASE_URL']
SIGN_IN_HEADERS = config['SIGN_IN_HEADERS']
SIGN_IN_PAYLOADS = config['SIGN_IN_PAYLOADS']
headers = config['headers']
params = config['params']
list_keys = config['list_keys']




def file_exists(file_name):
    return os.path.isfile(file_name)


def get_bearer_token():
    response = requests.post(SIGN_IN_URL, headers=SIGN_IN_HEADERS, params=SIGN_IN_PAYLOADS, timeout=30)
    dic_str = response.text
    token = ' '.join(['Bearer',dic_str.split('token')[1].split('"')[2]])
    return token

def update_bearer_token():
    try:
        page = requests.get(KEYWORD_URL, headers=headers, params=params, timeout=20)
        if page.status_code == 200:
            print('Token is valid!')
            return
        raise Exception(page.status_code)
    except Exception as inst: 
        if inst.args[0] == 401:
            print('Updating bearer token...')
            headers['authorization'] = get_bearer_token()
            configweb = {'SIGN_IN_URL': SIGN_IN_URL, 
                       'KEYWORD_URL': KEYWORD_URL, 
                       'SUPPLIERS_URL': SUPPLIERS_URL,
                       'PRODUCT_DATABASE_URL': PRODUCT_DATABASE_URL,
                       'SIGN_IN_HEADERS': SIGN_IN_HEADERS,
                       'SIGN_IN_PAYLOADS': SIGN_IN_PAYLOADS,
                       'headers': headers,
                       'params':params,
                       'list_keys': list_keys}
            with open("configWeb.json", "w") as json_file:
                json.dump(configweb, json_file)
        else:
            SystemExit(f'Error type {inst}')
        

def get_params(keyword, key, subkey, startFrom = None):
    if isinstance(keyword, str):
        params[key][subkey] = keyword
        if startFrom is not None:
            params[key]['from'] = startFrom
        return params[key]
    else:
        raise ValueError('the keyword needs to be a string!')



def js_request(keyword, url = KEYWORD_URL, key = 'keyword', subkey = 'search_terms', startFrom = None): 
    params = get_params(keyword, key, subkey, startFrom)
    for i in range(3): 
        try:
            #page = requests.get(url, headers=headers, proxies={'http': proxy, 'https': proxy}, params=params, timeout=60)
            page = requests.get(url, headers=headers, params=params, timeout=25)
            page.raise_for_status()
        except requests.exceptions.Timeout as etmout:
            if i == 2:
                pass#raise SystemExit(etmout)
            print('Request timed out. Trying to request again for {}. Try #{} of 3'.format(keyword, i+1))
            time.sleep(1)
        except requests.HTTPError as ehttp:
            if ehttp.response.status_code == 429 and i != 4:
                print('Reached request limit. Wait for 1 h.')
                time.sleep(3600)
                print('Requesting for page again. Try #{} of 4'.format(i+1))
            elif ehttp.response.status_code == 401:
                print('The Bearer token is invalid. Getting a new one...')
                try:
                    update_bearer_token()
                    print('Bearer Token has been updated')
                    time.sleep(1)
                except:
                    pass
            else: print(ehttp)
        except requests.exceptions.RequestException as err:
            print(err)
            break
        else: 
            return page.text


def transform_list_of_dicts(text, data_type = 'keyword', appendKeyword = None):
    if not isinstance(text, str):
        print('Page text returned belongs to {}'.format(type(text)))
        return []
    text = re.sub(r'(?<=:)null', r'-99999', text) 
    if data_type.lower() == 'keyword':
        text = re.sub(r'(?<=:)true', r'True', text)
        text = re.sub(r'(?<=:)false', r'False', text)
        new_text = re.split(r',(?={"score")', text)
        new_text[0] = re.split(r'\[(?={"score")', new_text[0])[1]
        new_text[-1] = new_text[-1].split(']')[0]
    elif data_type.lower() == 'supplier':
        text = re.sub(r'"total_shipments":\d+\}(?=\]|,)', '\g<0> !split!', text)
        new_text = text.split('!split!,')
        new_text[-1] = new_text[-1].split('!split!]')[0]
        new_text[0] = new_text[0].split('"data":[')[1]
    elif data_type.lower() == 'product database':
        text = re.sub(r'(?<=:)true', r'True', text)
        text = re.sub(r'(?<=:)false', r'False', text)
        total, new_text = text.split('"products":[')
        new_text = new_text.split('],"categories"')[0]
        new_text = re.split(r',(?={"id")', new_text)
        total_count = int(re.search(r'(?<="total_count":)\d+', total).group(0))
    list_of_dicts = []
    for rep in new_text:
        try:
            data_to_append = ast.literal_eval(rep)
            if appendKeyword is not None:
                data_to_append['Parent Keyword'] = appendKeyword
                data_to_append['Total NOP for the keyword'] = total_count
            list_of_dicts.append(data_to_append) 
        except:
            pass
    return list_of_dicts


def build_list_of_dicts(keyword, url = KEYWORD_URL, key = 'keyword', subkey = 'search_terms'):
    if isinstance(keyword, list):
        if not keyword:
            raise ValueError('Keyword list must not be empty')
        else:
            ans_list = []
            list_dics = map(build_list_of_dicts, key) 
            for element in list_dics:
                ans_list.extend(element)
            return ans_list
    print('requesting for {}'.format(keyword))
    if key in ['keyword', 'supplier']:
        try:
            data = js_request(keyword, url, key, subkey)
            return transform_list_of_dicts(data, data_type = key)
        except:
            return []
    elif key == 'product database':
        data = []
        for start in range(0,100,100):
            try:
                data.append(js_request(keyword, url, key, subkey, startFrom=start))
            except: 
                pass
        ans = []
        if data:
            for rep in data:
                ans.extend(transform_list_of_dicts(rep, data_type = key, appendKeyword = keyword)) 
        return ans
    


nest_asyncio.apply()    
async def concurrent_builder_dicts(taxonomy_words, key = 'keyword', m_workers=10):
    list_of_dicts = [] 
    with concurrent.futures.ThreadPoolExecutor(max_workers=m_workers) as executor: 
        try:
            loop = asyncio.get_event_loop()
        except:
            pass
        try:
            if key == 'keyword':
                    futures = [
                        loop.run_in_executor(executor, build_list_of_dicts, word)
                        for word in taxonomy_words
                        ]
            elif key == 'product database':
                    futures = [
                        loop.run_in_executor(executor, functools.partial(build_list_of_dicts, word, PRODUCT_DATABASE_URL, key,  'include_keywords'))
                        for word in taxonomy_words
                        ]
        except:
            pass
        for response in await asyncio.gather(*futures):
            try:
                for element in response:
                        list_of_dicts.append(element)
                        
            except:
                pass

    return list_of_dicts


    
def paralel_builder(taxonomy_words, key = 'keyword', m_workers=10):
    list_dict_keywords = []
    try: 
        loop = asyncio.get_event_loop()
        list_dict_keywords = loop.run_until_complete(concurrent_builder_dicts(taxonomy_words, key = key,  m_workers=m_workers))
    except:
        pass
    return list_dict_keywords

def remove_duplicates_from_list(lst_keywords, key = 'id'):
    d = defaultdict(list)
    final_lst = []
    for i in lst_keywords:
        d[i[key]].append(i)
    for i in d.values():
        final_lst.append(i[0])
    return final_lst


def save_file(data, file_name, method = 'json'):
    #func = {'ujson': ujson, 'json': json, 'pkl': pkl}
    way = 'w'
    if method == 'ujson':
        extension = 'json'
    else: 
        extension = method
        if method in ['pkl', 'pickle']:
            way = 'wb'
    file_name = '.'.join([file_name, extension])
    with open(file_name, way) as file:
        ujson.dump(data, file)
    
    

def webscraping_keywords(taxonomy_word):
    update_bearer_token()
    N = 3
    temp_names = taxonomy_word
    list_of_keywords = []
    while len(list_of_keywords) < 2*10**5:
        if (isinstance(temp_names, list) or isinstance(temp_names, np.ndarray)) and len(temp_names):
            try:
                iter_list = paralel_builder(temp_names)
                if not iter_list:
                    print('Error in getting new keywords! Trying again...')
                    temp_names = taxonomy_word
                else:
                    list_of_keywords.extend(iter_list)
                    if len(iter_list)<= N:
                        indices = list(range(len(iter_list)))
                    else:
                        indices = random.sample(range(0, len(iter_list)-1), N)
                    iter_list = [iter_list[ind] for ind in indices]
                    temp_names = [d['name'] for d in iter_list]
            except:
                pass
        else :
            temp = build_list_of_dicts(temp_names)
            list_of_keywords.extend(temp)
            temp_names = [d['name'] for d in temp]
    print('------------- Over ---------------')
    ans = remove_duplicates_from_list(list_of_keywords) 
    return ans


def webscraping_products(keyword_list):
    update_bearer_token()
    N = 5
    list_of_products = []
    if isinstance(keyword_list, str):
        list_of_products = paralel_builder(keyword_list, key = 'product database')
    elif isinstance(keyword_list, list) and keyword_list:
        for i in range(0, len(keyword_list), N): #python handles all the problems this expression could have
            keyword_names = keyword_list[i:i+N] #same
            try:
                iter_list = paralel_builder(keyword_names, key = 'product database')#, proxy = list_proxies[i%10])
                if not iter_list:
                    print('Error in getting products containing the keywords {}!'.format(keyword_names))
                else:
                    list_of_products.extend(iter_list)
            except:
                pass
           
    else: raise ValueError('The keywords provided need to be a string or a list of strings!')
    print('------------- Over ---------------')
    try:
        ans = remove_duplicates_from_list(list_of_products) 
        
    except:
        pass

    return ans

    

def transform_keyword_to_pd(list_of_data, sort_by = 'exactSearchVolume'):
    if not isinstance(list_of_data, list) or not list_of_data:
        raise ValueError('The data given must be a non-empty list')
    for element in list_of_data:
        list_to_null = np.setdiff1d(list(list_keys['keyword'].keys()),list(element.keys()))
        if len(list_to_null):
            for null_key in list_to_null:
                list_keys['keyword'][null_key].append(np.nan) 
        for non_null_key, value in element.items():
            value_to_add = np.nan if isinstance(value,int) and value == -99999 else value
            list_keys['keyword'][non_null_key].append(value_to_add) 
    temp_pd = pd.DataFrame( columns = list_keys['keyword'].keys() )
    for key, value in list_keys['keyword'].items():
        if key in ['exactSearchVolume', 'broadSearchVolume']:
            temp_pd[key] = np.array(value).astype(np.int32) 
        else: temp_pd[key] = np.asarray(value)
                  
    return temp_pd.sort_values(by=sort_by, ascending=False)#.set_index('name')


def transform_supplier_to_pd(list_of_data, sort_by = 'total_shipments'):
    if not isinstance(list_of_data, list) or not list_of_data:
        raise ValueError('The data given must be a non-empty list')
    for element in list_of_data:
        for non_null_key, value in element.items():
            list_keys['supplier'][non_null_key].append(value) 
    temp_pd = pd.DataFrame( columns = list_keys['supplier'].keys() )
    for key, value in list_keys['supplier'].items():
        if key == 'latest_shipment':
            temp_pd[key] = np.array(value).astype(np.int32) 
        else: temp_pd[key] = np.asarray(value)
                  
    return temp_pd.sort_values(by=sort_by, ascending=False).set_index('name')

def transform_product_database_to_pd(list_of_data, sort_by = 'net'):
    if not isinstance(list_of_data, list) or not list_of_data:
        raise ValueError('The data given must be a non-empty list')
    for element in list_of_data:
        list_to_null = np.setdiff1d(list(list_keys['product database'].keys()),list(element.keys()))
        if len(list_to_null):
            for null_key in list_to_null:
                list_keys['product database'][null_key].append(np.nan) 
        for non_null_key, value in element.items():
            value_to_add = np.nan if isinstance(value,int) and value == -99999 else value
            list_keys['product database'][non_null_key].append(value_to_add) 
    temp_pd = pd.DataFrame( columns = list_keys['product database'].keys() )
    for key, value in list_keys['product database'].items():
        if key in ['hasVariants', 'isUnavailable',  'noParentCategory', 'multipleSellers']:
            temp_pd[key] = np.array(value).astype(np.int32)
        else:
            temp_pd[key] = np.asarray(value)
                  
    return temp_pd.sort_values(by=sort_by, ascending=False)#.set_index('name')







