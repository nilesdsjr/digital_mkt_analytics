import os
import gc

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
        extract=Extract()
        transform=Transform()
        load=Load()
        self.log.info('Starting extraction of data from Github.')
        extraction=extract.url_extract(self.config['API']['default']['url'])
        self.log.info('Trying to unzip files {}.'.format(extraction))
        transform.unzip_file(extraction)
        self.log.info('Transforming extracted data into dataframes.')
        df_fb = transform.fb_ads_table()
        self.log.info('Uploading table facebook_ads_media_costs')
        load.load_to_db('stage', 'facebook_ads_media_costs', df_fb)
        del df_fb
        gc.collect()
        df_gl = transform.gl_ads_table()
        self.log.info('Uploading table google_ads_media_costs')
        load.load_to_db('stage', 'google_ads_media_costs', df_gl)
        del df_gl
        gc.collect()
        df_pg = transform.pgview_table()
        self.log.info('Uploading table pageviews')
        load.load_to_db('stage', 'pageviews', df_pg)
        del df_pg
        gc.collect()
        df_ld = transform.c_lead_table()
        self.log.info('Uploading table customer_leads_funnel')
        load.load_to_db('stage', 'customer_leads_funnel', df_ld)
        del df_ld
        gc.collect()
        self.log.info('Finished ETL execution.')

if __name__ == '__main__':

    etl=Etl()
    etl.run()