import sqlalchemy
import pandas as pd
from connection import Connection
from settings import LogStream


class Load:
    """Responsable for loading data into systems.
    """

    def __init__(self):
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def load_to_db(self, schema, table_name, df):
        """Loads data from pandas dataframe to Postgres
    
        Args:
          schema: Name of the destination schema.
          table_name: Name of the table.
          df: Dataframe with the data that is going to the db.
    
        Returns:
          Nothing.
    
        Raises:
          Exception: Re-raising pandas exception.
        """
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
