import pandas as pd
import urllib,json
import ndjson
import requests

meta = "https://redivis.com/api/tables/237"
info = "https://redivis.com/api/tables/237/data"

class RediPy (object):
    
    def __init__(self,meta_url,info_url,name=None):
        self.meta_url = meta_url
        self.info_url = info_url
        self.name=name
        
    def get_meta_url(self):
        return self.meta_url
    
    def set_meta_url(self,new_url):
        self.meta_url = new_url

    def get_info_url(self):
        return self.info_url
    
    def set_info_url(self,new_url):
        self.info_url = new_url
        
    def get_name(self):
        return self.name
    
    def set_name(self,new_name):
        self.name = new_name
    
    def import_meta(self):
        response = urllib.request.urlopen(self.meta_url)
        data = json.loads(response.read())
        data_variables = data["variables"]
        self.set_name(data["name"])
        return data_variables
    
    def meta_to_panda(self):
        datas = self.import_meta()
        ids = list(datas[0].keys())
        panda_butt = pd.DataFrame(datas,columns=ids)
        return panda_butt

    def import_info(self):
      response = requests.get(self.info_url)
      items = response.json(cls=ndjson.Decoder)
      initial_input = items[0]
      panda_paw = pd.DataFrame(initial_input,index=[0])
      i=1
      while i<len(items):
        panda_paw = panda_paw.append(items[i],ignore_index=True)
        i+=1

      return panda_paw

      #max_records = 5
      #df = pd.read_json(response, lines=True, chunksize=max_records)


#print(RediPy(meta).import_info())
print(RediPy(meta,info).import_info())

#print(RediPy(meta).meta_to_panda())