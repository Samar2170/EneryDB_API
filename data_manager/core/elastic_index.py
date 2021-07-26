import json
from .models import SeriesDetail, SeriesData
import os
from django.core.exceptions import ObjectDoesNotExist
from elasticsearch import Elasticsearch
import psycopg2
import pandas as pd


es=Elasticsearch(['localhost'], port=9200)


def cr_index():
    es.indices.create(index='eia', ignore=400)


def index_eia():
    DIR_PATH=(f'../Assets/Eia_Json/')
    files = os.listdir(DIR_PATH)
    for a in files:
        f = open(f'{DIR_PATH}{a}')
        content=f.read()
        splitcontent=content.splitlines()
        for i,s in enumerate(splitcontent):
            df=json.loads(s)
            try:
                descrp = df['description']
            except:
                descrp = 'N.A'    
            try:
                geog = df['geography']
            except:
                geog='N.A'
            try:
                seriesId = df['series_id']
            except:
                seriesId = 'N.A' 
            try:
                name=df['name']
            except:
                name='N.A'
            try:
                source=df['source']
            except:
                source='N.A' 

            data_array=[]            
            try:
                for a,b in df['data']:
                        dic={}
                        dic['period']=a
                        dic['val']=b
                        data_array.append(dic)
            except:
                dic={}
                dic['period']='N.A'
                dic['val']=1.0623
                data_array.append(dic)            

            try:
                data = {}
                data['name']=name
                data['code']=seriesId
                data['source']=source
                data['descrp']=descrp
                data['series']=data_array
                es.index(index='eia', id=i, doc_type='json', body=data)    
                print(f"index create for {df['series_id']}")
            except Exception as e:
                print(f"exception ---{str(e)}")    


conn = psycopg2.connect(user="admin",
                        password="admin@123",
                        host="localhost",
                        port=5432,
                        database="eia")


def cr_index_series_details():
    es.indices.create(index='eia_series_details', ignore=400)



def index_eia_series_details():
    sql=("SELECT * FROM core_seriesdetail")
    df=pd.read_sql(sql,conn)
    for i,r in df.iterrows():
        series={}
        series['name']=r['Name']
        series['code']=r['Code']
        series['id']=r['id']
        series['descrip']=r['Descrip']
        series['source']=r['source']
        es.index(index='eia_series_details', id=i, doc_type='son', body=series)

