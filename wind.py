# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 08:47:26 2023

@author: jlgon
"""

import requests
import pandas as pd
import csv

stream_status = False
df_wind = pd.DataFrame(columns=['id_cuidad',
                                    'date',
                                    'speed',
                                    'symbol',
                                    'symbolB',
                                    'gusts',
                                    ])
arrayAllLeads = []
stream_status = False

inicio = 1
final = 409
while inicio <= final:
    with open('id_url.csv', newline='') as archivo_csv:
        lector_csv = csv.reader(archivo_csv, delimiter=';', quotechar='"')
        for fila in lector_csv:
            correlativo = str(fila[0])
            if str(fila[0]) == str(inicio):
        
                url_archivo = fila[1]
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
                            wind = texto['wind'] # validar
                            speed = wind['speed']
                            symbol = wind['symbol']
                            symbolB = wind['symbolB']
                            gusts = wind['gusts']
                            row = {
                                'id_cuidad' : id_cuidad,
                                'date' : date,
                                'speed' : speed,
                                'symbol' : symbol,
                                'symbolB' : symbolB,
                                'gusts' : gusts,
                                }
                            df_wind = df_wind.append(row, ignore_index=True)
                            n = n+1
                    except:
                            n = n+1      
        inicio = inicio+1
df_wind.to_csv('C:/Users/jlgon/OneDrive/Escritorio/CoderHouse/wind.csv', index=False, encoding='utf-8')