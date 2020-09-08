import pandas as pd;

class Transformer:
   """
    A general class used to set path options for the runtime.
    It is used to access attributes dinamically through its object.

    ...

    Attributes
    ----------
    ROOT_DIR : str
        a formatted string specifying the path to the root's project. 
        It's determined by the main class' file location.
    CONFIG_PATH : str
        a formatted string specifying the path to the project's conf directory.
        Used to locate stored files like .yaml, .cfg, .ini, etc.
    LOG_DIR : str
        a formatted string specifying the path to the project's log directory.
        Stores the log file.

    Methods
    -------
    says(sound=None)
        Prints the animals name and what sound it makes
    """

    def __init__(self):
        pass
    

    def table_maker(self, extract):
        extract_list=extract[key]
        tb_tmp=pd.DataFrame(caged_list)
        return tb_tmp

        
        # columns=['categoria','cbo2002_ocupacao','competencia','fonte','grau_de_instrucao','horas_contratuais','id','idade','ind_trab_intermitente','ind_trab_parcial','indicador_aprendiz','municipio','raca_cor','regiao','salario','saldo_movimentacao','secao','sexo','subclasse','tam_estab_jan','tipo_de_deficiencia','tipo_empregador','tipo_estabelecimento','tipo_movimentacao','uf']