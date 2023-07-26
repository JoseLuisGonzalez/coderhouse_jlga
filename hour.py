# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 09:28:07 2023

@author: jlgon
"""

import requests
import pandas as pd
import csv

stream_status = False
df_wind = pd.DataFrame(columns=['id_cuidad',
                                    'date',
                                    'interval',
                                    'temp',
                                    'symbol_value',
                                    'symbol_description',
                                    'symbol_value2',
                                    'symbol_description2',
                                    #'wind',
                                    'rain',
                                    'humidity',
                                    'pressure',
                                    'clouds',
                                    'snowline',
                                    'windchill',
                                    'uv_index',
                                    ])
arrayAllLeads = []
stream_status = False

inicio = 1
final = 174
while inicio <= final:
    with open('id_url.csv', newline='') as archivo_csv:
        lector_csv = csv.reader(archivo_csv, delimiter=';', quotechar='"')
        for fila in lector_csv:
            correlativo = str(fila[0])
            if str(fila[0]) == str(inicio):
                #print(fila[1])
        
                url_archivo = fila[1]
                usuario = '&affiliate_id=nhixuwvv8492&v=3.0'
                url = url_archivo+usuario
                id_cuidad = url_archivo.replace('http://api.meteored.cl/index.php?api_lang=cl&localidad=', '')
                #print(url)

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
                            hour = texto['hour']
                            df = pd.DataFrame(hour, columns = ['interval','temp','symbol_value','symbol_description','symbol_value2','symbol_description2'
                                                               ,'wind','rain','humidity','pressure','clouds','snowline','windchill','uv_index'])
                            n2 = 0
                            while n2 <= 8:
                                id_cuidad = id_cuidad
                                interval = df.interval[n2]
                                temp = df.temp[n2]
                                symbol_value = df.symbol_value[n2]
                                symbol_description = df.symbol_description[n2]
                                symbol_value2 = df.symbol_value2[n2]
                                symbol_description2 = df.symbol_description2[n2]
                                #wind = df.wind[n2]
                                rain = df.rain[n2]
                                humidity = df.humidity[n2]
                                pressure = df.pressure[n2]
                                clouds = df.clouds[n2]
                                snowline = df.snowline[n2]
                                windchill = df.windchill[n2]
                                uv_index = df.uv_index[n2]
                                row = {
                                    'id_cuidad' : id_cuidad,
                                    'date' : date, 
                                    'interval' : interval,
                                    'temp' : temp,
                                    'symbol_value' : symbol_value,
                                    'symbol_description' : symbol_description,
                                    'symbol_value2' : symbol_value2,
                                    'symbol_description2' : symbol_description2,
                                    #'wind' : wind,
                                    'rain' : rain,
                                    'humidity' : humidity,
                                    'pressure' : pressure,
                                    'clouds' : clouds,
                                    'snowline' : snowline,
                                    'windchill' : windchill,
                                    'uv_index' : uv_index,
                                    }
                                df_wind = df_wind.append(row, ignore_index=True)
                                n2 = n2+1
                            n = n+1
                    except:
                            n = n+1      
        inicio = inicio+1