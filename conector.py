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