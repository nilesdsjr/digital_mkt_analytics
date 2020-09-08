import pandas as pd;

class Transformer:

    def __init__(self):
        pass
    

    def table_maker(self, extract):
        for item in extract:
            tb_tmp=pd.read_json('{"a":1,"b":2}\n{"a":3,"b":4}', lines=True)