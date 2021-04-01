import csv
import functions as f 


list_file_paths = f.list_files()

#create a new csv with all the data in event_data directory
f.create_csv(list_file_paths)


######## CASSANDRA SESSION AND KEYSPACE ###############
cluster = f.cassandra_clust()
sesion = f.cassandra_session(cluster)
f.create_keyspace(sesion)
f.set_keyspace(sesion)

########### CREATE QUERIES  #############################

query_1 = """CREATE TABLE IF NOT EXISTS song_description_by_session"""
query_1 = query_1 + """(session_id int, item_in_session int,artist varchar, song_title varchar,song_length float,
                        PRIMARY KEY(session_id,item_in_session))"""


query_2 = """CREATE TABLE IF NOT EXISTS song_playlist_session"""
query_2 = query_2 + """(user_id int,
                        session_id int,
                        item_in_session int,
                        user_first_name varchar,
                        user_last_name varchar,
                        artist varchar,
                        song_title varchar, 
                        PRIMARY KEY((user_id, session_id), item_in_session))
                        WITH CLUSTERING ORDER BY (item_in_session ASC)"""


query_3 = """CREATE TABLE IF NOT EXISTS usersname_listened_song"""
query_3 = query_3 + """(song_title varchar,
                        user_id int,
                        user_first_name varchar,
                        user_last_name varchar,
                        PRIMARY KEY(song_title,user_id))"""



########## CREATE THE 3 TABLES ####################
f.create_table(query_1, sesion)
f.create_table(query_2, sesion)
f.create_table(query_3, sesion)




####### INSERT DATA IN 3 TABLES ####################
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query_insert_1 = "INSERT INTO song_description_by_session (artist,song_title,song_length,session_id,item_in_session)"
        query_insert_1 = query_insert_1 + "VALUES(%s,%s,%s,%s,%s)"
        sesion.execute(query_insert_1, (line[0], line[9],float(line[5]), int(line[8]),int(line[3])))

        
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query_insert_2 = "INSERT INTO song_playlist_session (user_id,user_first_name,user_last_name,artist,song_title,session_id,item_in_session)"
        query_insert_2 = query_insert_2 + "VALUES(%s,%s,%s,%s,%s,%s,%s)"
        sesion.execute(query_insert_2, (int(line[10]),line[1],line[4],line[0], line[9],int(line[8]),int(line[3])))
        
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query_insert_3 = "INSERT INTO usersname_listened_song (user_id,user_first_name,user_last_name,song_title)"
        query_insert_3 = query_insert_3 + "VALUES(%s,%s,%s,%s)"
        sesion.execute(query_insert_3, (int(line[10]),line[1],line[4], line[9]))
        

sesion.shutdown()
cluster.shutdown()