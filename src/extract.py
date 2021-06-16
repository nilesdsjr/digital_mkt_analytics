import time
import os

from connection import Connection
from settings import LogStream, Settings

class Extract:
    """
    Responsable for the web extraction of web data.

    """
    def __init__(self):
        _log_stream=LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)
        settings = Settings()
        self.local_paths = settings.LOCAL_PATHS

    def url_extract(self, url):
        """ Executes get method from response.Session object.
        
        Parameters
        ----------
        url : str
            a formatted string specifying the API endpoint.

        Returns
        -------
        requests.Response Object
            
        Raises
        ------
        Exception
            Re-raises requests lib error.
        """
        t0=time.time()
        local_filename = os.path.join(self.local_paths['DATA_DIR'],'datasets.zip')
        conn=Connection()
        my_session=conn.request_retry_session()
        try:
            r=my_session.get(url, stream=True)
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
               for chunk in r.iter_content(chunk_size=8192): 
                   f.write(chunk)
        except Exception as e:
            self.log.error('API not responding right. Check the endpoint:{}'.format(url), exc_info=True)
            raise(e)
        t1=time.time()
        self.log.info('API connection took {} seconds. STATUS:{}'.format(t1-t0, r.status_code))
        return local_filename