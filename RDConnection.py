#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 18:36:13 2021

@author: Gustavo
"""

import psycopg2


class DataBaseConnection:
    def __init__(self):
        try:
            self.connection =  psycopg2.connect(
                host = 'rdspostgresjss.c6pzpx3eq8yn.eu-west-3.rds.amazonaws.com',
                port = 5432,
                user = 'rocket',
                password = 'Panther1.',
                database='BXB'
            )
            self.cursor= self.connection.cursor()  
            print('Successfully connected to the database!')
        except:
            print('Couldnt connect to the database')
    
    # def create_keyword_table(self):
    #     try:
    #         create_command =  """CREATE TABLE keyword_table(
    #         keywordName TEXT NOT NULL PRIMARY KEY,
    #         score FLOAT8,
    #         matches FLOAT8,
    #         keywordId TEXT NOT NULL,
    #         exactSuggestedBidMedian FLOAT8,
    #         avgGiveaway FLOAT8,
    #         exactAvgCpc FLOAT8,
    #         exactSearchVolume FLOAT8,
    #         estimatedBroadSearchVolume FLOAT8,
    #         keywordCountry VARCHAR(10),
    #         quarterlyTrend FLOAT8,
    #         estimatedAvgGiveaway FLOAT8,
    #         easeOfRankingScore FLOAT8,
    #         broadSearchVolume FLOAT8,
    #         broadSuggestedBidMedian FLOAT8,
    #         keywordCategory TEXT,
    #         monthlyTrend FLOAT8,
    #         broadAvgCpc FLOAT8,
    #         estimatedExactSearchVolume FLOAT8,
    #         keyword_url TEXT,
    #         hasUpdatedSearchVolume BOOLEAN,
    #         hasUpdatedCpc BOOLEAN,
    #         organicProductCount FLOAT8,
    #         sponsoredProductCount FLOAT8 );"""
            
    #         self.cursor.execute(create_command)
    #         self.connection.commit()
    #         print('Table created succesfully!')
    #     except Exception as error:
    #         print('Error trying to create table: {}'.format(error))

    
    # def create_products_database_table(self):
    #     try:
    #         create_command =  """CREATE TABLE products_database_table(
    #               productName TEXT NOT NULL, 
    #               productId TEXT NOT NULL PRIMARY KEY,
    #               nReviews FLOAT8, 
    #               estimatedSales FLOAT8, 
    #               productCountry TEXT, 
    #               weight FLOAT8, 
    #               weightUnit TEXT,
    #               state TEXT, 
    #               apiUpdatedAt FLOAT8, 
    #               imageUrl TEXT, 
    #               fees FLOAT8, 
    #               productSubCategory TEXT,
    #               productSubCategories TEXT,
    #               width FLOAT8, 
    #               dimensions TEXT,
    #               dimensionUnit TEXT,
    #               categoryNullifiedAt FLOAT8,
    #               estRevenue FLOAT8,
    #               scrapedAt TEXT, 
    #               productRating FLOAT8,
    #               tier TEXT, 
    #               hasVariants FLOAT8,
    #               rawCategory TEXT,
    #               sellerName TEXT, 
    #               nSellers FLOAT8,
    #               dimensionValuesDisplayData TEXT,
    #               productCategory TEXT, 
    #               isUnavailable FLOAT8, 
    #               listingQualityScore FLOAT8, 
    #               sellerType TEXT,
    #               listedAt FLOAT8,
    #               estimatedListedAt TEXT,
    #               length FLOAT8,
    #               noParentCategory FLOAT8,
    #               isSharedBSR FLOAT8, 
    #               color TEXT, 
    #               calculatedCategory TEXT, 
    #               asin TEXT,
    #               brand TEXT, 
    #               scrapedParentAsin TEXT,
    #               multipleSellers FLOAT8, 
    #               productRank FLOAT8, 
    #               pageAsin TEXT,
    #               height FLOAT8,  
    #               price FLOAT8,
    #               apiCategory TEXT,
    #               net FLOAT8,
    #               feeBreakdown TEXT, 
    #               variantAsinsCount FLOAT8,
    #               sampleVariants TEXT, 
    #               product_url TEXT, 
    #               bsr_product FLOAT8,
    #               hasRankFromApi FLOAT8,
    #               currency_code TEXT,  
    #               parentKeyword TEXT NOT NULL,
    #               totalNOPforKeyword INT NOT NULL,
    #               FOREIGN KEY (parentKeyword) REFERENCES keyword_table (keywordName));"""
            
    #         self.cursor.execute(create_command)
    #         self.connection.commit()
    #         print('Table created succesfully!')
    #     except Exception as error:
    #         print('Error trying to create table: {}'.format(error))    
    
    
    
    
    
    def insert_new_record_from_csv(self, table_name, path_csv):
        try:
            with open(path_csv, 'r') as csv_file:
                command_line = '''CREATE TEMP TABLE tmp_table 
                                    (LIKE {} INCLUDING DEFAULTS)
                                    ON COMMIT DROP;
                                    
                                    COPY tmp_table FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER '\t', NULL '', ENCODING 'utf-8');
                                    
                                    INSERT INTO {}
                                    SELECT *
                                    FROM tmp_table
                                    ON CONFLICT DO NOTHING'''.format(table_name, table_name);
                self.cursor.copy_expert(command_line, csv_file)
                self.connection.commit()
            print('Values inserted in {}'.format(table_name))
        except Exception as error:
            print('Error trying to insert values: {}'.format(error))
            return False
        else:
            return True
        
    def get_products_to_scrape(self):
        command_line = '''SELECT keywordName 
                        FROM   keyword_table
                        WHERE  keywordName NOT IN (SELECT parentKeyword FROM products_database_table) 
                        AND    exactSearchVolume <= 3000 AND exactSearchVolume >= 300
                        FETCH NEXT 6000 ROWS ONLY'''
        self.cursor.execute(command_line)
        return self.cursor.fetchall()
    
    
    def join_js_tables(self):
        command_line = '''SELECT * FROM products_database_table JOIN keyword_table ON products_database_table.parentkeyword = keyword_table.keywordname;'''
        self.cursor.execute(command_line)
        return self.cursor.fetchall()
        
    def query_data(self, table_name):
        command_line = '''SELECT * FROM {};'''.format(table_name)
        self.cursor.execute(command_line)
        return self.cursor.fetchall()
    
    def delete_table(self, table_name):
        try:
            command_line = '''DROP TABLE {}'''.format(table_name)
            self.cursor.execute(command_line)
            self.connection.commit()
            print('Table {} deleted'.format(table_name))
        except:
            pass
    
    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        self.connection.close()
        

    