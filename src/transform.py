import os

import pandas as pd
from zipfile import ZipFile

from settings import LogStream, Settings


class Transform:
    """
    Responsable for all data transformations.
    Mainly receives a dict and returns a queryable table.

    ...

    Attributes
    ----------
    extract_list : list
        A list containing all dicts from the specified json key.
    extract : dict
        The json content as dict
    key : str
        key name to be accessed.
    df : Pandas' DataFrame.
        Used different moments to load and manipulate the data from extract_list.
    dts : Pandas' DataFrame
       Removed comma and dot at the salario column. It stores only column salario.
    dtypes : dict
        Dictionary with all data types defined. It is used as parameters to convert datatypes.

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
                zip_ref.extractall(os.path.join(self.local_paths['DATA_DIR'], 'datasets'))
        except Exception as e:
            self.log.error('Something wrong unziping file {}'.format(zip_path), exc_info=True)
            raise(e)
    
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
  
        Args:
          json_path: Absolut path to the .json file.
  
        Returns:
          A pandas Dataframe.
  
        Raises:
          IOError: If something breaks during loading from local fils system.
        """
        try:
            df = pd.read_json(json_path)
        except IOError as e:
            self.log.error('Impossible to read file at: {}'.format(json_path), exc_info=True)
            raise(e)
        return df

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