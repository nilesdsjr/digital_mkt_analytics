import pandas as pd;

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
        return pd.DataFrame(extract_list)


        # columns=['categoria','cbo2002_ocupacao','competencia','fonte','grau_de_instrucao','horas_contratuais','id','idade','ind_trab_intermitente','ind_trab_parcial','indicador_aprendiz','municipio','raca_cor','regiao','salario','saldo_movimentacao','secao','sexo','subclasse','tam_estab_jan','tipo_de_deficiencia','tipo_empregador','tipo_estabelecimento','tipo_movimentacao','uf']