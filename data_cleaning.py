# from database_utils import DatabaseConnector
# from data_extraction import DataExtractor 
import sqlalchemy as sqla
import datetime
from datetime import date
import pandas as pd
import numpy as np
from validate_email import validate_email
import re

class DataCleaning:

    def __init__(self):
        pass
    '''
    DataCleaning uses methods to clean data from each of the data sources.
    '''
    def clean_user_data(self, df):
            #print(df.head())
        #print(df.info())
        #print(df.shape)
        df.replace("NULL", np.nan, inplace=True)
        df=df.dropna()
        #print(df.shape)
        df.drop(('index'),inplace=True, axis=1)
        #print(df.shape)

        #print(df.info())
        #print(df.head())

        #email
        mask=df['email_address'].apply(lambda x:validate_email(x))
        df=df[mask]
        #print(df)

        #country
        #print(df['country'].unique())
        #print(df['country_code'].unique())

        #country code
        #print(df.shape)
        #print(df.loc[df['country_code']=='GGB'])
        df.drop(df.loc[df['country_code']=='GGB'].index, inplace=True)
        #print(df.shape)
        #print(df['country_code'].unique())

        #print(df['date_of_birth'].head(10))
        df['date_of_birth']=pd.to_datetime(df['date_of_birth'],format="%Y/%m/%d", errors = "coerce")
        df['join_date']=pd.to_datetime(df['join_date'], format="%Y/%m/%d", errors = "coerce")
        #752, 866, 1021,
        #print(min(df['date_of_birth']))
        #print(max(df['date_of_birth']))

        # df['Year'] = df['date_of_birth'].dt.year.convert_dtypes()
        # df['Month'] = df['date_of_birth'].dt.month.convert_dtypes()
        # df['Day'] = df['date_of_birth'].dt.day.convert_dtypes()
        #print(df.shape)

        #df1=df[(df['date_of_birth'] > df['join_date'])]
        #print(df1)
        #print(df1[['date_of_birth', 'join_date']])

        df.drop(df.loc[(df['date_of_birth'] > df['join_date'])].index, inplace=True)
        #print(df.shape)


    #Check phone_number
        df['phone_number'] = df['phone_number'].str.replace('\W','')
        df['phone_number'] = df['phone_number'].str.replace(r"[a-zA-Z]", "", regex=True)
        print(df)
        return df
    
    # def clean_card_data(self, ext=DataExtractor()):
    #     card_data = ext.retrieve_pdf_data()
    #     return card_data


# dc = DataCleaning()

# #####################  CLEAN USER DATA    #############################################
# df = dc.clean_user_data()








###############################   CLEAN CARD DATA ###########################################
# df1=dc.clean_card_data()
# print(type(df1))

