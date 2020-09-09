from transformer import Transformer
from loader import Loader
from extractor import Extractor

class Etl:

    def run(self):
        _extraction=Extractor()
        _tranformation=Transformer()
        extraction=_extraction.url_extract()
        if extraction['success']:
            transformation=_tranformation.table_maker(extraction['caged'])
            print(transformation)
        else:
            transformation=_tranformation.table_maker(extraction['caged'])
            print(transformation)



if __name__ == '__main__':

    etl=Etl()
    etl.run()