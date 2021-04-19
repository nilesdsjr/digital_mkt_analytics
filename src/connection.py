import time
import pyodbc
from sqlalchemy import create_engine
from settings import Configuration, LogStream
from retry_requests import retry
from requests import Session


class Connection:
    """
    Responsable for getting connections with databases and APIs.
    Attributes
    ----------
    host : str
        hostname where postgresql is served.
    port : int
        port where postgresql is accessable.
    database : str
        name of the database wanted for connection.
    user : str
        db user.
    pswd : str
        user password.
    Methods
    -------
    psql_conn(self)
        Connects to postgresql using psycopg2 and returns connection object.
    sql_engine(self)
        Connects to postgresql using sqlalchemy and returns a engine object.
    requests_retry_session
    """
    def __init__(self):
        configuration=Configuration()
        config=configuration.load_config()
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)
        self.psql=config['profile']['postgres']
        self.mssql=config['profile']['mssql']

    def sql_engine(self):
        engine = create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(
            self.psql['user'],
            self.psql['pswd'],
            self.psql['host'],
            self.psql['port'],
            self.psql['database'])
            )
        return engine

    def mssql_conn(self):
        try:
            conn=pyodbc.connect(
                driver=self.mssql['driver'],
                server=self.mssql['server'],
                database=self.mssql['database'],
                uid=self.mssql['user'],
                pwd=self.mssql['pswd']
                )
        except pyodbc.DatabaseError as e:
            self.log.error('MS SQL Server connection failed.',
                      exec_info=True)
            raise(e)
        return conn

    def request_retry_session(self):
        rt=retry(Session(), retries=5, backoff_factor=0.2)
        return rt.request_retry_session()

