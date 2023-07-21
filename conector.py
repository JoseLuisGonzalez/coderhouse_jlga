# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 23:11:04 2023

@author: jlgon
"""

import redshift_connector

def connector():
    conn = redshift_connector.connect(
         host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
         database='data-engineer-database',
         port=5439,
         user='jl_gonzalezaguila_coderhouse',
         password='9t7ZiTxQb9'
      )
    return conn

# cursor = conn.cursor()
# cursor.execute("select max(id) from jl_gonzalezaguila_coderhouse.cuidad_chile")
# result: tuple = cursor.fetchall()
# print(result)

# conn.close()

# CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad(
#   id BIGINT IDENTITY NOT NULL, /* Cannot be overridden */
#   url varchar(255) ,
#   cuidad varchar(255)
# );

# CREATE TABLE jl_gonzalezaguila_coderhouse.cuidad_chile(
#   id int NOT NULL, /* Cannot be overridden */
#   url varchar(255) ,
#   cuidad varchar(255)
# );

# CREATE TABLE jl_gonzalezaguila_coderhouse.clima(
#   id_cuidad varchar(255) NOT NULL, /* Cannot be overridden */
#   status varchar(255) ,
#   location varchar(255),
#   date varchar(255),
#   name varchar(255),
#   month varchar(255),
#   symbol_value varchar(255),
#   symbol_description varchar(255),
#   symbol_value2 varchar(255),
#   symbol_description2 varchar(255),
#   tempmin varchar(255),
#   tempmax varchar(255),
#   rain varchar(255),
#   humidity varchar(255),
#   pressure varchar(255),
#   snowline varchar(255),
#   uv_index_max varchar(255),
#   local_time varchar(255),
#   local_time_offset varchar(255)
# );


# SELECT *
# FROM jl_gonzalezaguila_coderhouse.clima;

#https://docs.devart.com/odbc/redshift/python.htm   
    