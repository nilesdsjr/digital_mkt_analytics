import pandas as pd
pd.set_option('display.max_columns', 500)
import numpy as np

class Transformer:
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

    Methods
    -------
    table_maker(self, extract, key)
        Receives a dict and a key to return a structured table as Pandas dataframe.
    """

    def __init__(self):
        pass


    def table_maker(self, extract_list):
        df=pd.DataFrame()
        dt=pd.DataFrame(extract_list)
        print(dt)
        dts=dt['salario'].str.replace(',','').str.replace('.','')
        dt=dt.drop(columns='salario')
        dt['salario']=dts
        print(dt)
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
        print(dt.dtypes)
        return dt