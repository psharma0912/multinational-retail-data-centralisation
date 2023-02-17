import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
import sqlalchemy as sqla
import tabula
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

class DatabaseConnector:
    '''
    create a class DatabaseConnector which will be used to connect with and upload data to the database.
        '''
    def __init__(self):
        pass
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as stream:
            creds = yaml.safe_load(stream)
            print(creds)
            return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(("postgresql+psycopg2://" + creds["RDS_USER"] + ":" + creds["RDS_PASSWORD"] + "@" + creds["RDS_HOST"] + ":" + str(creds["RDS_PORT"]) + "/" + creds["RDS_DATABASE"]))
        print(engine)
        return engine
    
    def list_db_tables(self):
        eng = self.init_db_engine()
        eng.connect()
        inspector = inspect(eng)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df:pd.DataFrame, dim_users):
        #url = ('postgresql://postgres:password@localhost:5432/Sales_Data')
        url = 'postgresql+psycopg2://postgres:pnpnsn@localhost:5432/Sales_Data'
        engine = sqla.create_engine(url)
        #engine = self.init_db_engine()
        df.to_sql(dim_users, engine,  if_exists= 'replace')
        #engine.execute("SELECT * FROM dim_users").fetchall()   


if __name__ == '__main__':
    databaseconnector = DatabaseConnector()  
    engine = databaseconnector.init_db_engine()
    tables = databaseconnector.list_db_tables()
    print(tables)

    databaseextractor = DataExtractor()
    data = databaseextractor.read_rds_table(engine, tables)
    print(data.head())

    datacleaning = DataCleaning()
    clean_users = datacleaning.clean_user_data(data)
    databaseconnector.upload_to_db(clean_users, 'dim_users')
    print(clean_users)
    # print(df)

    #dbc.upload_to_db(tables, 'dim_users')

      
 


 


  
        
   