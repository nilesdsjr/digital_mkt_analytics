import pandas as pd
from connection import Connection
from conf.settings import LogStream

class Loader:

    def __init__(self):
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def load_to_db(self, table_name, df):

        dt=pd.DataFrame()
        dt=df
        try:
            self.log.info('Starting upload to table {}'.format(table_name))
            self.log.info('Starting connection with database')
            conn=Connection()
            engine=conn.sql_engine()
            self.log.info('Sending dataframe to database')
            dt.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            self.log.info('Dataframe to database Sent.')

        except Exception as e:
            self.log.error('Load dataframe to database Failed.', exc_info=True)
            raise(e)

