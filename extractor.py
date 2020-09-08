import requests as r

''' Responsable for the online extraction of JSON data.'''
class Extractor:
    
    def __init__(self):
        self.url='http://dataeng.quero.com:5000/caged-data'

    def url_extract(self):
        response=r.get(self.url)
        self.resp_info=response.headers['content-type']
        if self.resp_info=="application/json":
            return response.json()
        else:
            print("Unsupported content-type.")
        return