import os

from transform import Transform
from load import Load
from extract import Extract
from settings import Configuration, LogStream, Settings

class Etl:
    """Responsable for calling the ETL dataflow
    """

    def __init__(self):
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)
        settings = Settings()
        self.local_paths = settings.LOCAL_PATHS
        configuration = Configuration()
        self.config = configuration.load_config()

    def run(self):
        """ Executes the ETL.
        
        Returns
        -------
        Nothing
            
        Raises
        ------
        Nothing
        """
        self.log.info('\n\n######## STARTING SIMPLE ETL TO LOCAL DB #########\n')
        self.log.info('Initiating classes.')
        _extract=Extract()
        _transform=Transform()
        _load=Load()
        self.log.info('Starting extraction of data from Github.')
        extraction=_extract.url_extract(self.config['API']['default']['url'])
        self.log.info('Trying to unzip files {}.'.format(extraction))
        _transform.unzip_file(extraction)
        self.log.info('Transforming extracted data into dataframes.')

        #carregar os arquivos
        #transformar em tabela
        #subir para o db
        
        #load=_load.load_to_db('quero_api', 'caged', transformation)


        self.log.info('######## FINISHED SIMPLE ETL TO LOCAL DB #########')


if __name__ == '__main__':

    etl=Etl()
    etl.run()