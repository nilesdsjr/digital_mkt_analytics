from extractor import Extractor
from transformer import Transformer
from loader import Loader

class Etl:

    def run(self):
        _extraction=Extractor()
        extraction=_extraction.url_extract()
        _tranformation=Transformer()
        transformation=_tranformation.table_maker(extraction)



if __name__ == '__main__':