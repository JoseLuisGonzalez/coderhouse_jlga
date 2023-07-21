# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 09:23:44 2023

@author: jlgon
"""

import requests
import pandas as pd
import csv
import redshift_connector
#from conector import connector
import datetime

conn = redshift_connector.connect(
     host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
     database='data-engineer-database',
     port=5439,
     user='jl_gonzalezaguila_coderhouse',
     password='9t7ZiTxQb9'
  )

stream_status = False
df_moon = pd.DataFrame(columns=['id_cuidad',
                                    'date',
                                    'in',
                                    'out',
                                    'lumi',
                                    'desc',
                                    'symbol',
                                    'fecha_carga',
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
                            moon = texto['moon']
                            in_moon = moon['in']
                            out = moon['out']
                            lumi = moon['lumi']
                            desc = moon['desc']
                            symbol = moon['symbol']
                            row = {
                                'id_cuidad' : id_cuidad,
                                'date' : date,
                                'in' : in_moon,
                                'out' : out,
                                'lumi' : float(lumi.replace('%','').rstrip())/100,
                                'desc' : desc.replace('%','').replace(',','').replace('"','').rstrip(),
                                'symbol' : symbol,
                                'fecha_carga' : datetime.datetime.now(),
                                }
                            df_moon = df_moon.append(row, ignore_index=True)
                            n = n+1
                    except:
                            n = n+1      
        inicio = inicio+1


def write_dataframe_to_redshift(df, table_name, conn):
    cursor = conn.cursor()

    for index, row in df.iterrows():
        insert_query = f"""
            INSERT INTO {table_name} (id_cuidad, fecha ,in_moon ,out_moon ,lumi_moon ,desc_moon ,symbol_moon, fecha_carga)
            VALUES ({row['id_cuidad']}, '{row['date']}', '{row['in']}', '{row['out']}', {row['lumi']}, '{row['desc']}', '{row['symbol']}', '{row['fecha_carga']}');
        """
        cursor.execute(insert_query)

    conn.commit()
    cursor.close()

write_dataframe_to_redshift(df_moon, 'jl_gonzalezaguila_coderhouse.cuidad_moon', conn)
