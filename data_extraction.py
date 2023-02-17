from sqlalchemy import inspect
import pandas as pd
import sqlalchemy
#from database_utils import DatabaseConnector 
import tabula
import numpy as np

class DataExtractor:
    '''
    This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
    '''    
    def __init__(self):
        pass

    def read_rds_table(self, engine, table_list):
        data = pd.read_sql_table(table_list[1],engine)
        #print(type(data))
        return data

    def retrieve_pdf_data(self):
        pdf_path='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        link = tabula.read_pdf(pdf_path, pages="all", lattice=True, guess=True)
        link[0].columns = ["card_number", "expiry_date", "card_provider", "date_payment_confirmed"]
        clean_df = pd.DataFrame(columns=["card_number", "expiry_date", "card_provider", "date_payment_confirmed"])
        for i in range(len(link)): #307 dataframes in the list
            df = link[i]
            clean_df = pd.concat([clean_df, df], axis=0)
        return clean_df
        
# ext=DataExtractor() 
# ext.read_rds_table()

# # read_pdf returns list of DataFrames
# df1=ext.retrieve_pdf_data()
# print(df1.shape)


