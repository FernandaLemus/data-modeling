# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster
import functions as f 


cluster = f.cassandra_clust()
sesion = f.cassandra_session(cluster)
f.set_keyspace(sesion)


##SELECT statement to verify the data was entered into song_description_by_session table
query = """SELECT artist,song_title,song_length FROM song_description_by_session WHERE session_id = 338 AND item_in_session = 4"""
result_1 = sesion.execute(query)   
for row in result_1:
    print(row.artist,row.song_title,row.song_length, 'Result query 1')
    

    
    
## SELECT statement to verify the data was entered into song_playlist_session table
query = """SELECT artist,song_title,user_first_name,user_last_name, item_in_session
            FROM song_playlist_session WHERE user_id =10 AND session_id = 182"""
result_2 = sesion.execute(query)
for row in result_2:
    print(row.artist,row.song_title,row.user_first_name, row.user_last_name,row.item_in_session,'Result query 2')
    
    
## SELECT statement to verify the data was entered into usersname_listened_song table
query = """SELECT user_first_name,user_last_name
            FROM usersname_listened_song WHERE song_title = 'All Hands Against His Own' 
            """
result_3 = sesion.execute(query)
for row in result_3:
    print(row.user_first_name, row.user_last_name, 'Result query 3')
    

sesion.shutdown()
cluster.shutdown()