import json
from .models import SeriesDetail, SeriesData
import os
from django.core.exceptions import ObjectDoesNotExist

def save_eia_bulk():
    DIR_PATH=(f'../Assets/Eia_Json/')
    files = os.listdir(DIR_PATH)
    for a in files:
        f = open(f'{DIR_PATH}{a}')
        content=f.read()
        splitcontent=content.splitlines()
        for s in splitcontent:
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
            try:
                sd = SeriesDetail.objects.get(Code=seriesId)
                sdd = SeriesData.objects.filter(series_id=sd.id)
                print(f'{sd.Name} already loaded')
            except ObjectDoesNotExist:                        
                try:
                    SeriesDetail.objects.get_or_create(Code=seriesId,Name=name,
                                                        Descrip=descrp,Geography=geog,
                                                        source=source)
                    so = SeriesDetail.objects.get(Code=seriesId)
                    print(f'{so.Name} entered')
                    df_data = df['data']
                    for a,b in df_data:
                        S=SeriesData(series=so, period=a, data=float(b))                        
                        S.save() 
                    print(f'data added for {so.Name}')    
                except Exception as e:
                    nme = df['name']
                    print(f'exception for {nme} {e}')