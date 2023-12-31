# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 16:28:52 2023

@author: jlgon
"""

import requests
import pandas as pd
import csv
#import redshift_connector
import datetime
from conector import connector


conn = connector()

stream_status = False
df_clima = pd.DataFrame(columns=['id_cuidad','status',
                                             'location',
                                             'fecha',
                                             'dia_semana',
                                             'mes',
                                             'symbol_value',
                                             'symbol_description',
                                             'symbol_value2',
                                             'symbol_description2',
                                             'tempmin',
                                             'tempmax',
                                             'rain',
                                             'humidity',
                                             'pressure',
                                             'snowline',
                                             'uv_index_max',
                                             'local_time',
                                             'local_time_offset',
                                             'fecha_carga',
                                             ])
arrayAllLeads = []
stream_status = False


cursor = conn.cursor()
cursor.execute("select id,url from jl_gonzalezaguila_coderhouse.cuidad_url")
result: pd.DataFrame = cursor.fetch_dataframe()
total_registros = len(result.index)
cursor.close()

inicio = 1
final = total_registros
while inicio <= final:
    url = result.loc[result['id'] == inicio]
    url_archivo = url['url'].to_string(index=False)

    usuario = '&affiliate_id=nhixuwvv8492&v=3.0'
    url = url_archivo+usuario
    id_cuidad = url_archivo.replace('http://api.meteored.cl/index.php?api_lang=cl&localidad=', '')

    response = requests.request("GET", url_archivo+usuario)
    
    n = 1
    while n <= 5:
        try:
            if response.status_code == 200:
                response_json = response.json()
                status = response_json['status']
                location = response_json['location']
                texto = response_json['day'][str(n)]
                date = texto['date']
                name = texto['name']
                month = texto['month']
                symbol_value = texto['symbol_value']
                symbol_description = texto['symbol_description']
                symbol_value2 = texto['symbol_value2']
                symbol_description2 = texto['symbol_description2']
                tempmin = texto['tempmin']
                tempmax = texto['tempmax']
                rain = texto['rain']
                humidity = texto['humidity']
                pressure = texto['pressure']
                snowline = texto['snowline']
                uv_index_max = texto['uv_index_max']
                local_time = texto['local_time']
                local_time_offset = texto['local_time_offset']
                row = {
                    'id_cuidad' : id_cuidad,
                    'status' : status,
                    'location' : location.replace('[Región Metropolitana de Santiago;Chile]','').replace('[Región de Valparaíso;Chile]',''),
                    'fecha' : date,
                    'dia_semana' : name,
                    'mes' : month,
                    'symbol_value' : symbol_value,
                    'symbol_description' : symbol_description,
                    'symbol_value2' : symbol_value2,
                    'symbol_description2' : symbol_description2,
                    'tempmin' : tempmin,
                    'tempmax' : tempmax,
                    'rain' : rain,
                    'humidity' : humidity,
                    'pressure' : pressure,
                    'snowline' : snowline,
                    'uv_index_max' : uv_index_max,
                    'local_time' : local_time,
                    'local_time_offset' : local_time_offset,
                    'fecha_carga' : datetime.datetime.now(),
                    }
                df_clima = df_clima.append(row, ignore_index=True)
                n = n+1
        except:
                n = n+1      
inicio = inicio+1
        
def write_dataframe_to_redshift(df, table_name, conn):
    cursor = conn.cursor()

    for index, row in df.iterrows():
        insert_query = f"""
            INSERT INTO {table_name} (id_cuidad, status ,location ,fecha ,dia_semana ,mes, symbol_value,symbol_description, symbol_value2,symbol_description2,tempmin,tempmax,rain,humidity,pressure,snowline,uv_index_max,local_time,local_time_offset,fecha_carga)
            VALUES ({row['id_cuidad']}, {row['status']}, '{row['location']}', {row['fecha']}, '{row['dia_semana']}', '{row['mes']}', '{row['symbol_value']}', '{row['symbol_description']}', '{row['symbol_value2']}', '{row['symbol_description2']}', {row['tempmin']}, {row['tempmax']}, {row['rain']}, {row['humidity']}, {row['pressure']}, {row['snowline']}, {row['uv_index_max']}, '{row['local_time']}', {row['local_time_offset']}, '{row['fecha_carga']}');
        """
        cursor.execute(insert_query)

    conn.commit()
    cursor.close()

write_dataframe_to_redshift(df_clima, 'jl_gonzalezaguila_coderhouse.clima', conn)  