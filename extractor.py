from connection import Connection
from conf.settings import LogStream
import time

class Extractor:
    """
    Responsable for the web extraction of JSON data.

    ...

    Attributes
    ----------
    response : str
        HTTP response from GET request.
    url : str
        a formatted string specifying the API endpoint.
    t0 : timestamp
        Marks the begining of the request.
    t1 : timestamp
        Marks the ending of the request.

    Methods
    -------
    url_extract(self)
        Returns the json response as dict. Doesn't return any other type.
    """
    def __init__(self):
        self.url='http://dataeng.quero.com:5000/caged-data'
        _log_stream=LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def url_extract(self):
        t0=time.time()
        conn=Connection()
        my_session=conn.request_retry_session()
        try:
            r=my_session.get(self.url)
        except Exception as e:
            self.log.error('API not responding right. Check the endpoint:{}'.format(self.url))
            raise(e)
        t1=time.time()
        self.log.info('API connection took {} seconds. STATUS:{}'.format(t1-t0, r.status_code))
        ct_type=r.headers['content-type']
        if ct_type=="application/json":
            return r.json()
        else:
            self.log.info(r.text)
            self.log.error('Unsupported content-type {} - Only JSON is supported!'.format(ct_type))
            raise AttributeError('Unsupported content-type {} - Only JSON is supported!'.format(ct_type))