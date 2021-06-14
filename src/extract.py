from connection import Connection
from settings import LogStream
import time

class Extract:
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
        _log_stream=LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def url_extract(self, url):
        t0=time.time()
        conn=Connection()
        my_session=conn.request_retry_session()
        try:
            r=my_session.get(url, stream=True)
        except Exception as e:
            self.log.error('API not responding right. Check the endpoint:{}'.format(self.url), exc_info=True)
            raise(e)
        t1=time.time()
        self.log.info('API connection took {} seconds. STATUS:{}'.format(t1-t0, r.status_code))
        return r.response