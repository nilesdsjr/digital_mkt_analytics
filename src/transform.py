import os
import json
import pandas as pd
from zipfile import ZipFile

from settings import LogStream, Settings


class Transform:
    """
    Responsable for all data transformations.
    Mainly receives a dict and returns a queryable table.
    
    Methods
    -------
    table_maker(self, extract, key)
        Receives a dict and a key to return a structured table as Pandas dataframe.
    """

    def __init__(self):
        _log_stream = LogStream()
        self.log = _log_stream.log_stream(origin=__class__.__name__)
        settings = Settings()
        self.local_paths = settings.LOCAL_PATHS
        self.local_files = {
            'facebook_ads_media_costs.jsonl': os.path.join(self.local_paths['DATA_DIR'], 'datasets', 'facebook_ads_media_costs.jsonl'),
            'google_ads_media_costs.jsonl': os.path.join(self.local_paths['DATA_DIR'], 'datasets', 'google_ads_media_costs.jsonl'),
            'pageview.txt': os.path.join(self.local_paths['DATA_DIR'], 'datasets', 'pageview.txt'),
            'customer_leads_funnel.csv': os.path.join(self.local_paths['DATA_DIR'], 'datasets', 'customer_leads_funnel.csv')
        }

    def table_maker(self, extract_list):
        df=pd.DataFrame()
        dt=pd.DataFrame(extract_list)
        dts=dt['salario'].str.replace(',','').str.replace('.','')
        dt=dt.drop(columns='salario')
        dt['salario']=dts
        dtypes ={'categoria':'int16',
                 'cbo2002_ocupacao':'int32',
                 'competencia':'int32',
                 'fonte':'int16',
                 'grau_de_instrucao':'int16',
                 'horas_contratuais':'int32',
                 'id':'int16',
                 'idade':'int16',
                 'ind_trab_intermitente':'int16',
                 'ind_trab_parcial':'int16',
                 'indicador_aprendiz':'int16',
                 'municipio':'int32',
                 'raca_cor':'int16',
                 'regiao':'int16',
                 'salario':'float32',
                 'saldo_movimentacao':'int32',
                 'secao':'string',
                 'sexo':'int16',
                 'subclasse':'int32',
                 'tam_estab_jan':'int16',
                 'tipo_de_deficiencia':'int16',
                 'tipo_empregador':'int16',
                 'tipo_estabelecimento':'int16',
                 'tipo_movimentacao':'int16',
                 'uf':'int16'}
        dt=dt.astype(dtype=dtypes)
        self.log.info(dt.dtypes)
        return dt

    def unzip_file(self, zip_path):
        try:
            with ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.local_paths['DATA_DIR'])
        except Exception as e:
            self.log.error('Something wrong unziping file {}'.format(zip_path), exc_info=True)
            raise(e)
    
    def txt2str(self, txt_path):
        """Reads a .txt file and loads as string.
  
        Args:
          txt_path: Absolut path to the .txt file.
  
        Returns:
          str object.
  
        Raises:
          IOError: If something breaks during loading from local fils system.
        """
        try:
            with open(txt_path, 'r') as f:
                txt_str = f.read()
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(txt_path), exc_info=True)
            raise(e)
        return txt_str
    
    def csv2df(self, csv_path):
        """Reads a .csv file and loads as pandas DataFrame.
  
        Args:
          csv_path: Absolut path to the .csv file.
  
        Returns:
          A pandas Dataframe.
  
        Raises:
          IOError: If something breaks during loading from local fils system.
        """
        try:
            df = pd.read_csv(csv_path)
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(csv_path), exc_info=True)
            raise(e)
        return df

    def json2df(self, json_path):
        """Reads a .json file and loads as pandas DataFrame.
  
        Arguments
        ---------
          json_path: Absolut path to the .json file.
  
        Returns
        -------
          A pandas Dataframe.
  
        Raises
        -------
          IOError: If something breaks during loading from local fils system.
        """
        try:
            df = pd.DataFrame()
            json_str = self.txt2str(json_path)
            for line in iter(json_str.splitlines()):
                line_dict = json.loads(line)
                df = df.append(line_dict, ignore_index=True)
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(json_path), exc_info=True)
            raise(e)
        return df


    def fb_ads_table(self):

        """ Reads a .json file and structures it into a dataframe table.
        
            This method is specific to transform the facebook_ads_media_costs.jsonl

        Returns
        -------
        pandas.Dataframe Object
            A dataframe with a formatted table containing the facebook ads data.
            
        Raises
        ------
        Exception
            Re-raises lib error.
        """
        try:
            fb_df = self.json2df(self.local_files['facebook_ads_media_costs.jsonl'])
        except Exception as e:
            self.log.error('Impossible to create dataframe table.', exc_info=True)
            raise(e)
        return fb_df

    def gl_ads_table(self):
        """ Reads a .json file and structures it into a dataframe table.
 
        Returns
        -------
        pandas.Dataframe Object
            A dataframe with a formatted table containing the Google ads data.
            
        Raises
        ------
        Exception
            Re-raises lib error.
        """
        try:
            gl_df = self.json2df(self.local_files['google_ads_media_costs.jsonl'])
        except Exception as e:
            self.log.error('Impossible to create dataframe table.', exc_info=True)
            raise(e)
        return gl_df

    def pgviews_access_cleanse(self, pg_value):
        """ Reads a string value to remove unused data.
        
        Parameters
        ----------
        pg_value : str
            String value that shows access point from user.

        Returns
        -------
        item: str
            Formatted str
            
        Raises
        ------
        Exception
            Returns null.
        """
        try:
            item = pg_value.split('?', 1)[1]
        except:
            return
        return item
 
    def pgviews_device_id_cleanse(self, pg_value):
        """ Reads a string value to remove unused data.
        
        Parameters
        ----------
        pg_value : str
            String value that shows the device id from user.

        Returns
        -------
        item: str
            Formatted str
            
        Raises
        ------
        Exception
            Returns null.
        """
        try:
            item = pg_value.split(' device_id:', 1)[1]
        except:
            return
        return item    

    def pgviews_referer_cleanse(self, pg_value):
        """ Reads a string value to remove unused data.
        
        Parameters
        ----------
        pg_value : str
            String value that shows referer url from user.

        Returns
        -------
        item: str
            Formatted str
            
        Raises
        ------
        Exception
            Returns null.
        """
        try:
            item = pg_value.split(' referer:', 1)[1]
        except:
            return
        return item  

    def pgview_table(self):
        """ Reads a .txt file and structures it into a dataframe table.

        Returns
        -------
        pandas.Dataframe Object
            
        Raises
        ------
        Exception
            Re-raises lib error.
        """
        pg_str = self.txt2str(self.local_files['pageview.txt'])
        pg_lines = list()
        pg_items = list()
        pg_dicts = {}
        #Breaks string by breakline and adds as a list item to pg_lines
        for line in iter(pg_str.splitlines()):
            pg_lines.append(line)
        #Breaks the whole line into 3 smaller items and adds a list to a list.
        for item in pg_lines:
            items = item.split('|')
            pg_items.append(items)
        #Extracts the smaller list from a list of lines and creates a list of dicts.
        for index, value in enumerate(pg_items):
            access = self.pgviews_access_cleanse(value[0])
            device_id = self.pgviews_device_id_cleanse(value[1])
            referer = self.pgviews_referer_cleanse(value[2])
            pg_dicts[index]={'access': access, 'device_id': device_id, 'referer': referer}
        pg_df = pd.DataFrame.from_dict(pg_dicts, 'index')

        dtypes = {
            'access': 'string',
            'device_id': 'string',
            'referer': 'string'
            }
        pg_df=pg_df.astype(dtype=dtypes)
        return pg_df

    def c_lead_table(self):
        """ Reads a .csv file and structures it into a dataframe table.

        Returns
        -------
        requests.Response Object
            
        Raises
        ------
        Exception
            Re-raises requests lib error.
        """        
        try:
            ld_df = self.csv2df(self.local_files['customer_leads_funnel.csv'])
        except Exception as e:
            self.log.error('Impossible to create dataframe table.', exc_info=True)
            raise(e)
        return ld_df