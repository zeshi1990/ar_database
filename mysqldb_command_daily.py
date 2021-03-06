#!/usr/bin/env python
from __future__ import print_function
# coding: utf-8

# In[ ]:

__author__ = "zeshi"

import os
import time
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
import mysql.connector
from mysql.connector import errorcode
from mysqldb_level0 import populate_data_server
from level0_2_level1 import level0_to_level1_data_merge
from level1_cleaning import level1_cleaning_site

print("Running start time is", datetime.now())
# rsync data from webserver and transfer data from local to local
os.system("python /media/raid0/zeshi/AR_db/tmp_to_server_data.py")

print("Finished transfer data from tmp to server_data!")

# Query site_names from mysql and populate server data into level_0 table
cnx = mysql.connector.connect(user = "root", password = "root", database = "ar_data")
cursor = cnx.cursor()
try:
    cursor.execute("SELECT site_name FROM sites")
    site_names = cursor.fetchall()
except mysql.connector.Error as err:
    print(err)
cursor.close()
cnx.close()

site_names_list = []

for site_name in site_names:
    site_names_list.append(site_name[0])

# Initialize parallel processing
pool = Pool(processes=cpu_count())
# Mapping populate data server with the site_names_list
pool.map(populate_data_server, site_names_list)
pool.close()
pool.join()

# Query site_name and number of node at each site
cnx = mysql.connector.connect(user = "root", password = "root", database = "ar_data")
cursor = cnx.cursor()
try:
    cursor.execute("SELECT site_name, num_of_nodes FROM sites")
    sites_infos = cursor.fetchall()
except mysql.connector.Error as err:
    print(err)
cursor.close()
cnx.close()

def merge0_to_1_parallel(site_info):
    starting_time = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    ending_time = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    site_name = site_info[0]
    site_num_of_nodes = site_info[1]
    for node_id in range(1, site_num_of_nodes + 1):
        level0_to_level1_data_merge(site_name, node_id, datetime_range_interupt=(starting_time, ending_time))
    
# Initizalize parallel processing
pool = Pool(processes=cpu_count())
# Merge level_0 data to level_1 data
pool.map(merge0_to_1_parallel, sites_infos)
pool.close()
pool.join()

# Clean level_1 data
starting_time = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
ending_time = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
for site_id in range(1, 14):
	level1_cleaning_site(site_id, starting_time, ending_time)
	print("Finished cleaning data from site", site_id)


# In[ ]:

def long_term_update(starting_time, real_ending_time):
    # Query site_infos, site_name and number of nodes
    cnx = mysql.connector.connect(user = "root", password = "root", database = "ar_data")
    cursor = cnx.cursor()
    try:
        cursor.execute("SELECT site_name, num_of_nodes FROM sites")
        sites_infos = cursor.fetchall()
    except mysql.connector.Error as err:
        print(err)
    cursor.close()
    cnx.close()
    
    # ending_time is 7 days later than starting date
    ending_time = starting_time + timedelta(days=7)
    while ending_time <= real_ending_time:
        print(starting_time, ending_time)
        for site_info in sites_infos:
            site_name = site_info[0]
            site_num_of_nodes = site_info[1]
            for node_id in range(1, site_num_of_nodes + 1):
                level0_to_level1_data_merge(site_name, node_id, datetime_range_interupt=(starting_time, ending_time))
        starting_time += timedelta(days=7)
        ending_time += timedelta(days=7)

