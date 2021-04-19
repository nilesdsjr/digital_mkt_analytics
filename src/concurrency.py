from settings import Settings, LogStream, Configuration
import concurrent.futures

class Pool:
    """
    Responsable for creating a pool of threads

    Attributes
    ----------
    host : str
        hostname where postgresql is served.

    Methods
    -------
    psql_conn(self)
        Connects to postgresql using psycopg2 and returns connection object.
    """
    def __init__():
        configuration=Configuration()
        config=configuration.load_config()
        _log_stream=LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)
        self.URLS = {
            'url': config['API']['default']['url']
        }

    def task_sub(self, task, urls):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(task, url, 60): url for url in urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    return future.result()
                except Exception as e:
                    self.log.error('{} generated an exception: {%s}'.format(url, exc))
                else:
                    self.log.info('{} page is {} bytes'.format(url, len(data)))