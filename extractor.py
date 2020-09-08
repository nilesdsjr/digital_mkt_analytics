from connection import Connection
from settings import LogStream
import requests as r

class Extractor:
   """
    Responsable for the online extraction of JSON data. 

    ...

    Attributes
    ----------
    ROOT_DIR : str
        a formatted string specifying the path to the root's project. 
        It's determined by the main class' file location.
    CONFIG_PATH : str
        a formatted string specifying the path to the project's conf directory.
        Used to locate stored files like .yaml, .cfg, .ini, etc.
    LOG_DIR : str
        a formatted string specifying the path to the project's log directory.
        Stores the log file.

    Methods
    -------
    says(sound=None)
        Prints the animals name and what sound it makes
    """
    
    def __init__(self):
        self.url='http://dataeng.quero.com:5000/caged-data/500'
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def url_extract(self):
        
        t0 = time.time()
        try:
            self.log.info('Trying to get API data.')
            response=r.get(self.url)
        except requests.exceptions.Timeout:
            self.log.warning('API timeout. Retrying connection.')
            response=Connection.requests_retry(self.url)
        except requests.exceptions.RequestException as e:
            # catastrophic error. bail.
            raise SystemExit(e)
        t1 = time.time()
        self.log.info('API connection took ', t1 - t0, ' seconds')
        ct_type=response.headers['content-type']
        if ct_type=="application/json":
            return response.json()
        else:
            self.log.error('Unsupported content-type {}'.format(ct_type))
            raise AttributeError('Unsupported content-type {}'.format(ct_type))