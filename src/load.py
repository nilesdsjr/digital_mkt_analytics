import sqlalchemy
import pandas as pd
from connection import Connection
from settings import LogStream


class Load:

    def __init__(self):
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def load_to_db(self, schema, table_name, df):
        dt=pd.DataFrame()
        dt=df
        try:
            self.log.info('Starting upload to table {}'.format(table_name))
            self.log.info('Starting connection with database')
            conn=Connection()
            engine=conn.sql_engine()
            self.log.info('Sending dataframe c database')
            dt.to_sql(
                name=table_name,
                 con=engine,
                 schema=schema,
                 if_exists='append',
                 index=False
                 )
            self.log.info('Dataframe to database Sent.')

        except Exception as e:
            self.log.error('Load dataframe to database Failed.', exc_info=True)
            raise(e)
    
    def csv2df(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(csv_path), exc_info=True)
            raise(e)
        return df

    def json2df(self, json_path):
        try:
            df = pd.read_json(json_path)
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(json_path), exc_info=True)
            raise(e)
        return df

    def txt2str(self, txt_path):
        try:
            with open(txt_path, 'r') as f:
                txt_str = f.read()
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(txt_path), exc_info=True)
            raise(e)
        return txt_str
