# Documentation 

This repository contains the following files:
1. data (log_data and song_data datasets files)
2. create_tables.py (creates a sparkify database and songplays, users, songs, time and artist tables)
3. etl.ipyn (jupiter notebook with a step by step guide of the etl.py process)
4. etl.py (the ETL process that extracts the information from the json files and puts it into tables)
5. sql_queries.py (contains all the queries for creation, insertion and drop tables)
6. test.ipynb (tests the data insertion in the corresponding tables)



## On how to run this process
    1. First write *run create_tables.py* in a pthon console. This creates a database and the necessary tables for the successful execution of the process.
    2. Write *run etl.py* in a python console. In this step the process that feeds the tables will begin. More details about the process are below.



5 tables were created to facilitate the extraction of information about user activity on Sparkify through queries. This information is structured with the idea of analyzing the listened songs by the users.

## Two data sources were considered:
    - A Song Dataset
    - Log files records with page NextSong(from app users) based on the songs in the dataset above

The principal output of this pipeline is the songplays table which incorporate information about the users and the song they've played, including the information of the artist of these songs. In this way we can querying all the information of interest from a single table.

You can find a brief description of those tables here below

### songplays table:
records in log data associated with song plays
songplay_id: song identification number
start_time: playback start time 
user_id: user (who played the song) identification number
level: user subscrption level (free/paid)
song_id: played song identification number
artist_id: artist identification number
session_id: user session identification number
location: artist location
user_agent: browser from which the user accesses

### users table: 
users in the app
user_id: user identification number
first_name: user first name
last_name: user last name
gender: user gender (M/F)
level: level of user subscription (free/paid)

### songs table: 
songs in music database
song_id: song identification number
title: title of the song
artist_id: artist identification number 
year: year of the song 
duration: duration of the song (seconds)

### artists table
artists in music database
artist_id, artist_name, artist_location, artist_latitude, artist_longitude 

### time
timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

You can find a diagram for the relationship model [here](https://dbdiagram.io/d/5fe00df79a6c525a03bbc81f).



