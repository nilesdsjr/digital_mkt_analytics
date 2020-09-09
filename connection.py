import time
from sqlalchemy import create_engine
from conf.settings import Configuration, LogStream
from retry_requests import retry
from requests import Session
from psycopg2 import connect, OperationalError, Error


class Connection:
    """
    Responsable for getting connections with databases and APIs.

    ...

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
        self.host=config['profile']['postgres']['host']
        self.port=config['profile']['postgres']['port']
        self.database=config['profile']['postgres']['database']
        self.user=config['profile']['postgres']['user']
        self.pswd=config['profile']['postgres']['pswd']

    def psql_conn(self):
        try:
            con=connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.pswd
            )
        except OperationalError as e:

            self.log.error('Postgresql connection failed.',
                      exec_info=True)

            raise(e)
        return con

    def sql_engine(self):
        engine = create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(
            self.user,
            self.pswd,
            self.host,
            self.port,
            self.database))


    def request_retry_session(self):
        return retry(Session(), retries=5, backoff_factor=0.2)

