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


#############################################################################################
#############################################################################################
################### procesing the files in event_data directory #############################
#############################################################################################
#############################################################################################


def list_files(direc='event_data'):
    """
    function that lists the file names in a specified directory (default: event_data)
    """
    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/'+ direc
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
    return file_path_list


def create_csv(file_path_list): 
    """
    description: create a new csv file with the information in all the files in a specified directory
    arguments: 
        file_path_list = list of file names (output of list_files function)
    """  
    #empty list of rows that will be generated from each file
    full_data_rows_list = [] 
    #for every filepath in the file path list 
    for f in file_path_list:
    #reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
    # extracting each data row one by one and append it        
            for line in csvreader:
                full_data_rows_list.append(line) 
    # creating a smaller event data csv file called event_datafile_new csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
        'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''): #skip whitespace
                continue
            writer.writerow((row[0], row[2], row[3], int(row[4]), row[5], float(row[6]), row[7], row[8], int(row[12]), row[13], int(row[16])))


        
        
#############################################################################################
#############################################################################################
########################### CREATE TABLES AND INSERT DATA ###################################
#############################################################################################
#############################################################################################

def cassandra_clust():
    cluster = Cluster()
    return cluster

def cassandra_session(cluster):
    """Create connection to cassandra instance"""
    session = cluster.connect()
    return session


def create_keyspace(session):
    """create a keyspace with name _name_, if no name the default will be sparkify
    arguments: name of the keyspace and a cassandra session object (output of the cassandra_session function)"""
    #Create a Keyspace 
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkify
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
    except Exception as e:
        print(e)

def set_keyspace(session):
    try:
        session.set_keyspace('sparkify')
    except Exception as e:
        print(e)


def create_table(query, session):
    """create a table with the statement in query via cassandra session object """
    try:
        session.execute(query)
    except Exception as e:
        print(e)
        
        