import requests
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from settings import Configuration, LogStream
from requests.packages.urllib3.util.retry import Retry
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
        

    def requests_retry_session(
        self,
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(401, 500, 502, 504)
        ):
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
    
    def requests_retry(self, url):
        t0 = time.time()
        try:
            response = self.requests_retry_session().get(url)
        except Exception as x:
            self.log.error('Retried 3 times and failed :', x.__class__.__name__)
        else:
            self.log.info('Success retrying : ', response.status_code)
        finally:
            t1 = time.time()
            self.log.info('Retrying session took ', t1 - t0, ' seconds')