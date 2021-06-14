from transform import Transform
from load import Load
from extract import Extract
from settings import LogStream

class Etl:

    def __init__(self):
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)

    def run(self):
        self.log.info('######## STARTING SIMPLE ETL TO LOCAL DB #########')
        self.log.info('Initiating classes.')
        _extraction=Extractor()
        _tranformation=Transformer()
        _load=Loader()
        self.log.info('Starting extraction JSON data from API.')
        extraction=_extraction.url_extract()
        if extraction['success']:
            self.log.info('JSON file has Status SUCCESS to TRUE.')
            self.log.info('Starting to transform JSON data into one table.')
            transformation=_tranformation.table_maker(extraction['caged'])
            self.log.info('transform JSON data into one table finished.')
            self.log.info('Starting to load table into database.')
            load=_load.load_to_db('quero_api', 'caged', transformation)
        else:
            self.log.info('JSON file has Status SUCCESS to FALSE.')
            self.log.info('Starting to transform JSON data into one table.')
            transformation=_tranformation.table_maker(extraction['caged'])
            self.log.info('transform JSON data into one table finished.')
            self.log.info('Starting to load table into database.')
            load=_load.load_to_db('caged_no_sucess', transformation)

        self.log.info('######## FINISHED SIMPLE ETL TO LOCAL DB #########')


if __name__ == '__main__':

    etl=Etl()
    etl.run()