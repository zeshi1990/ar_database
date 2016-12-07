
# coding: utf-8

# In[32]:

from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import numpy as np
from datetime import datetime, date
import os


# In[33]:

# This script is for initialize the database.
def init_db():
    DB_NAME = 'ar_data'

    TABLES = {}

    TABLES['sites'] = (
        "CREATE TABLE `sites` ("
        "  `site_id` int NOT NULL AUTO_INCREMENT,"
        "  `site_name` varchar(14) NOT NULL,"
        "  `num_of_nodes` int NOT NULL,"
        "  PRIMARY KEY (`site_id`)"
        ") ENGINE=InnoDB")

    
    TABLES['motes'] = (
        "CREATE TABLE `motes` ("
        "  `mote_id` int(3) NOT NULL AUTO_INCREMENT,"
        "  `site_id` int(2) NOT NULL,"
        "  `node_id` int(2) NOT NULL,"
        "  `lat` float NOT NULL,"
        "  `lon` float NOT NULL,"
        "  `elevation` float NOT NULL,"
        "  `put_time` date NOT NULL,"
        "  `mac` varchar(14) NOT NULL,"
        "  `sd_last_update` datetime,"
        "  `server_last_update` datetime,"
        "  `sd_level_1` datetime NOT NULL,"
        "  `server_level_1 datetime` datetime NOT NULL,"
        "  `ground_dist` float,"
        "  PRIMARY KEY (`site_id`,`node_id`), UNIQUE KEY `mote_id` (`mote_id`)"
        ") ENGINE=InnoDB")

    cnx = mysql.connector.connect(user='root', password='root')
    cursor = cnx.cursor()

    # Create the database
    def create_database(cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    try:
        cnx.database = DB_NAME    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)

    # Create two Tables, sites and motes
    for name, ddl in TABLES.iteritems():
        try:
            print("Creating table {}: ".format(name), end='')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()
    cnx.close()

    # Popoluate sites into the sites table
    site_info = pd.read_csv("/media/raid0/zeshi/AR_db/server_data/site_node_info.csv", header=0, sep=",")
    site_names = np.unique(site_info["site_id"].as_matrix())
    sites = ()
    for site_name in site_names:
        temp_site = (site_name, )
        sites = sites + (temp_site,)

    cnx = mysql.connector.connect(user='root', password='root', database="ar_data")
    cursor = cnx.cursor()
    try:
        cursor.execute("ALTER TABLE sites AUTO_INCREMENT = 1")
        add_site = ("INSERT INTO sites "
                    "(site_name) "
                    "VALUES (%s)")
        cursor.executemany(add_site, sites)
        cnx.commit()
    except mysql.connector.Error as err:
        print("Failed populating data into motes: {}".format(err))
    cursor.close()
    cnx.close()

    # Populate motes into the sites table
    motes = ()
    query = ("SELECT site_id FROM sites WHERE site_name = %s")
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    try:
        for idx, row in site_info.iterrows():
            site_name = (row["site_id"],)
            cursor.execute(query, site_name)
            site_id = cursor.fetchone()
            if site_id:
                temp_motes = (site_id[0], )
                temp_motes = temp_motes + (row["node_id"], 
                                           row["lat"], 
                                           row["long"], 
                                           row["elev"], 
                                           datetime.now().strftime("%Y-%m-%d"), row["mac"])
                motes = motes + (temp_motes,)
            else:
                print("The site name of " + site_name + " does not exist!")
    except mysql.connector.Error as err:
            print("Failed querying data from sites: {}".format(err))
    cursor.close()
    cnx.close()

    cnx = mysql.connector.connect(user='root', password='root', database="ar_data")
    cursor = cnx.cursor()
    try:
        cursor.execute("ALTER TABLE motes AUTO_INCREMENT = 1")
        add_mote = ("INSERT INTO motes "
                    "(site_id, node_id, lat, lon, elevation, put_time, mac) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        cursor.executemany(add_mote, motes)
        cnx.commit()
    except mysql.connector.Error as err:
            print("Failed populating data into motes: {}".format(err))
    cursor.close()
    cnx.close()
    
    # Create a table called level_0, which is used to store all parsed level_0 data
    TABLE_ts = (
        "CREATE TABLE `level_0` ("
        "  `site_id` int NOT NULL,"
        "  `node_id` int NOT NULL,"
        "  `datetime` datetime NOT NULL,"
        "  `voltage` float,"
        "  `temperature` float,"
        "  `relative_humidity` float,"
        "  `soil_moisture_1` float,"
        "  `soil_temperature_1` float,"
        "  `soil_ec_1` float,"
        "  `soil_moisture_2` float,"
        "  `soil_temperature_2` float,"
        "  `soil_ec_2` float,"
        "  `snowdepth` float,"
        "  `judd_temp` float,"
        "  `unname_1` float,"
        "  `unname_2` float,"
        "  `solar` float,"
        "  `maxibotics` float,"
        "  `sd_card` tinyint(1),"
        "  FOREIGN KEY (`site_id`, `node_id`) REFERENCES motes(`site_id`, `node_id`),"
        "  KEY (`datetime`)"
        ") ENGINE=InnoDB")

    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    try:
        print("Creating table {}: ".format('level_0'), end='')
        cursor.execute(TABLE_ts)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

    cursor.close()
    cnx.close()
    
    # Create a table called level_1, which is used to store all parsed level_0 data
    TABLE_ts_1 = (
        "CREATE TABLE `level_1` ("
        "  `site_id` int NOT NULL,"
        "  `node_id` int NOT NULL,"
        "  `datetime` datetime NOT NULL,"
        "  `voltage` float,"
        "  `temperature` float,"
        "  `relative_humidity` float,"
        "  `soil_moisture_1` float,"
        "  `soil_temperature_1` float,"
        "  `soil_ec_1` float,"
        "  `soil_moisture_2` float,"
        "  `soil_temperature_2` float,"
        "  `soil_ec_2` float,"
        "  `snowdepth` float,"
        "  `judd_temp` float,"
        "  `solar` float,"
        "  `maxibotics` float,"
        "  `sd_clean` float,"
        "  `tmp_clean` float,"
        "  `rh_clean` float,"
        "  `sd_card` tinyint(1),"
        "  FOREIGN KEY (`site_id`, `node_id`) REFERENCES motes(`site_id`, `node_id`),"
        "  KEY (`datetime`)"
        ") ENGINE=InnoDB")

    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    try:
        print("Creating table {}: ".format('level_1'), end='')
        cursor.execute(TABLE_ts_1)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

    cursor.close()
    cnx.close()


# # The function below should read and parse server data files

# In[34]:

def parse_server_file(fn, site_id, node_id, server_last_update):
    # Read the file
    f = open(fn, "r")
    
    # initialize the mode of the data as 12
    mode = 14
    
    # initialize the output of the data
    output = ()
    new_server_last_update = server_last_update
    old_line = "~~~"
    # Start processing each line of the data from the bottom of the file
    for line in reversed(list(f)):

        # Filter out the row starting with "~" 
        # this line of data is just recording the meta information of the node
        if line[0] == "~":
            old_line = line
            continue

        else:
            line_list = line.split(",")
            length_new = len(line_list)
            # Only use the rows whose length is larger or equal to 10
            if length_new >= 10:

                # Some data starting with the mode of 14, detect them, if they change to 14
                # keep using them
                if length_new == 12 and mode == 14 and old_line[0] == "~":
                    mode = 12
                # Try split the data string and parse the first two items
                # If failed, continue to the next row of data
                try:
                    time_str = line_list[0].split(":")
                    date_str = line_list[1].split("/")
                    line_datetime = datetime(int(date_str[2]), int(date_str[0]), int(date_str[1]),
                                        int(time_str[0]), int(time_str[1]), int(time_str[2]))
                except:
                    continue
                
                # If the time stamp is larger than now, continue to the next row of data
                if line_datetime > datetime.now() or line_datetime < datetime(2013, 1, 1):
                    continue
                    
                # If the time stamp is smaller or equal than the last update time
                if server_last_update and line_datetime <= server_last_update:
                    break
                
                # If line_datetime is larger than the last time update the db, record new last update time
                if (new_server_last_update is None) or (line_datetime > new_server_last_update):
                    new_server_last_update = line_datetime
                
                temp_line = (site_id, node_id, line_datetime)
                
                # Voltage, temperature, relative_humidity, soil moisture temperature ec #12, index 2 - 10
                for idx in range(2, 11):
                    try:
                        if float(line_list[idx]) != float('Inf'):
                            temp_line += (float(line_list[idx]), )
                        else:
                            temp_line += (None, )
                    except:
                        temp_line += (None, )
                if mode == 12:
                    # Snow depth
                    try:
                        if float(line_list[-1]) != float('Inf'):
                            temp_line += (float(line_list[-1]), )
                        else:
                            temp_line += (None, )
                    except:
                        temp_line += (None, )
                        
                    # Solar radiation
                    temp_line += (None, )
                    
                    # For caples, need to add maxibotics
                    if site_id == 3:
                        temp_line += (None, )
                    
                else:
                    # Snow depth
                    try:
                        if float(line_list[-2]) != float('Inf'):
                            temp_line += (float(line_list[-2]), )
                        else:
                            temp_line += (None, )
                    except:
                        temp_line += (None, )
                    
                    # Solar radiation
                    try:
                        if float(line_list[-1]) != float('Inf'):
                            temp_line += (float(line_list[-1]), )
                        else:
                            temp_line += (None, )
                    except:
                        temp_line += (None, )
                    
                    # For caples, need to add maxibotics
                    if site_id == 3:
                        try:
                            if float(line_list[-3]) != float('Inf'):
                                temp_line += (float(line_list[-3]), )
                            else:
                                temp_line += (None, )
                        except:
                            temp_line += (None, )
                    
                temp_line += (0, )
                output = (temp_line, ) + output
            old_line = line
    if output == ():
        return None
    else:
        return (output, new_server_last_update)


# # The function below populate data from the folder having all server data

# In[35]:

def populate_data_server(site_name):
    print("Start populating server data into mysql at "+site_name)
    files = os.listdir("/media/raid0/zeshi/AR_db/server_data/"+site_name)
    if ".DS_Store" in files:
        files.remove(".DS_Store")
    node_query = ("SELECT site_id, node_id, put_time, server_last_update FROM motes WHERE mac = %s")
    site_name_query = ("SELECT site_name FROM sites WHERE site_id = %s")
    update_table_motes = ("UPDATE motes SET server_last_update = %s WHERE site_id = %s AND node_id = %s AND mac = %s")
    insert_table_level_0 = ("INSERT INTO level_0 "
                            "(site_id, node_id, datetime, voltage, temperature, relative_humidity, "
                            "soil_moisture_1, soil_temperature_1, soil_ec_1, soil_moisture_2, "
                            "soil_temperature_2, soil_ec_2, snowdepth, solar, sd_card) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    insert_table_level_0_caples = ("INSERT INTO level_0 "
                            "(site_id, node_id, datetime, voltage, temperature, relative_humidity, "
                            "soil_moisture_1, soil_temperature_1, soil_ec_1, soil_moisture_2, "
                            "soil_temperature_2, soil_ec_2, snowdepth, solar, maxibotics, sd_card) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    for f in files:
        mac = f[:-4].split("_")
        mac = mac[-3] + mac[-2] + mac[-1]
        print(site_name + ": " + mac)
        try:
            cursor.execute(node_query, (mac,))
        except mysql.connector.Error as err:
            print("Querying error happens when trying to query mac address " + f)
            print(err)
            continue
        
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            print("No node found to mac address: " + mac +" from " + site_name)
            continue
        max_row = rows[0]
        if len(rows) > 1:
            for row in rows[1:]:
                if row[2] > max_row[2]:
                    max_row = row
        site_id = max_row[0]
        node_id = max_row[1]
        server_last_update = max_row[3]
        try:
            cursor.execute(site_name_query, (site_id, ))
        except mysql.connector.Error as err:
            print("Querying error happens when trying to query site id " + str(site_id))
            print(err)
            continue 
        db_site_name = cursor.fetchone()[0]
        if db_site_name != site_name:
            print("The neomote with mac adress: " + mac + "has been swapped to another site.")
            continue
        fn = "/media/raid0/zeshi/AR_db/server_data/" + site_name + "/" + f
        new_items = parse_server_file(fn, site_id, node_id, server_last_update)
        if new_items:
            update_data = new_items[0]
            new_server_last_update = new_items[1]
            try:
                cursor.execute(update_table_motes, (new_server_last_update, site_id, node_id, mac))
                if site_name != "Caples_Lk":
                    cursor.executemany(insert_table_level_0, update_data)
                else:
                    cursor.executemany(insert_table_level_0_caples, update_data)
                cnx.commit()
            except mysql.connector.Error as err:
                print("Error happens when inserting data!")
                print(err)
                print(update_data)
                break
    cursor.close()
    cnx.close()


# # The function below should read and parse the SD card data files

# In[43]:

def parse_sd_file(fn, site_id, node_id, sd_last_update):
    # Read the file
    f = open(fn, "r")
    
    # initialize the mode of the data as 12
    mode = 17
    
    # initialize the output of the data
    output = ()
    new_sd_last_update = sd_last_update
    # Start processing each line of the data from the bottom of the file
    for line in reversed(list(f)):
        line_list = line.split(",")
        length_new = len(line_list)
        # Only use the rows whose length is larger or equal to 10， except for Owens_Camp
        if length_new == 9 and site_id == 9:
            try:
                time_str = line_list[0].split(":")
                date_str = line_list[1].split("/")
                line_datetime = datetime(int(date_str[2]), int(date_str[0]), int(date_str[1]),
                                         int(time_str[0]), int(time_str[1]), int(time_str[2]))
            except:
                continue
                
            # If the time stamp is larger than now, continue to the next row of data
            if line_datetime > datetime.now() or line_datetime < datetime(2013, 1, 1):
                continue
            
            if sd_last_update and line_datetime <= sd_last_update:
                break
            
            # If line_datetime is larger than the last time update the db, record new last update time
            if (new_sd_last_update is None) or (line_datetime > new_sd_last_update):
                new_sd_last_update = line_datetime

            temp_line = (site_id, node_id, line_datetime)
            
            # Voltage, temperature, relative_humidity, soil moisture temperature ec #12, index 2 - 10
            for idx in range(2, 5):
                try:
                    if float(line_list[idx]) == -99. or line_list[idx] == 'inf':
                        temp_line += (None, )
                    else:
                        temp_line += (float(line_list[idx]), )
                except:
                    temp_line += (None, )
            
            for idx in range(5, 11):
                temp_line += (None, )
            
            # Snow depth
            try:
                temp_line += (float(line_list[5]), )
            except:
                temp_line += (None, )
                
            # Judd temp
            try:
                temp_line += (float(line_list[6]), )
            except:
                temp_line += (None, )
                
            # Unname_1
            try:
                temp_line += (float(line_list[7]), )
            except:
                temp_line += (None, )
                
            # Unname_2
            try:
                temp_line += (float(line_list[8]), )
            except:
                temp_line += (None, )
                
            # Solar radiation
            temp_line += (None, )
            # Maxibotics
            temp_line += (None, )
            # SD card flag
            temp_line += (1, )
            
            output = (temp_line, ) + output
            
        if length_new == 15 or length_new == 17:

            # Some data starting with the mode of 14, detect them, if they change to 14
            # keep using them
            if length_new == 15 and mode == 17 :
                mode = 15

            # Try split the data string and parse the first two items
            # If failed, continue to the next row of data
            try:
                time_str = line_list[0].split(":")
                date_str = line_list[1].split("/")
                line_datetime = datetime(int(date_str[2]), int(date_str[0]), int(date_str[1]),
                                    int(time_str[0]), int(time_str[1]), int(time_str[2]))
            except:
                continue

            # If the time stamp is larger than now, continue to the next row of data
            if line_datetime > datetime.now() or line_datetime < datetime(2013, 1, 1):
                continue

            # If the time stamp is smaller or equal than the last update time
            if sd_last_update and line_datetime <= sd_last_update:
                break

            # If line_datetime is larger than the last time update the db, record new last update time
            if (new_sd_last_update is None) or (line_datetime > new_sd_last_update):
                new_sd_last_update = line_datetime

            temp_line = (site_id, node_id, line_datetime)

            # Voltage, temperature, relative_humidity, soil moisture temperature ec #12, index 2 - 10
            for idx in range(2, 11):
                try:
                    if float(line_list[idx]) == -99. or line_list[idx] == 'inf':
                        temp_line += (None, )
                    else:
                        temp_line += (float(line_list[idx]), )
                except:
                    temp_line += (None, )
            
            # Snow depth
            try:
                temp_line += (float(line_list[11]), )
            except:
                temp_line += (None, )
                
            # Judd temp
            try:
                temp_line += (float(line_list[12]), )
            except:
                temp_line += (None, )
                
            # Unname_1
            try:
                temp_line += (float(line_list[13]), )
            except:
                temp_line += (None, )
                
            # Unname_2
            try:
                temp_line += (float(line_list[14]), )
            except:
                temp_line += (None, )

            if mode == 15:
                # Solar radiation
                temp_line += (None, )
                # Maxibotics
                temp_line += (None, )
            
            if mode == 17:
                # Solar radiation
                try:
                    temp_line += (float(line_list[15]), )
                except:
                    temp_line += (None, )

                # Maxibotics
                try:
                    temp_line += (float(line_list[16]), )
                except:
                    temp_line += (None, )

            temp_line += (1, )
            output = (temp_line, ) + output
    if output == ():
        return None
    else:
        return (output, new_sd_last_update)


# In[44]:

def populate_data_sd(site_name):
    print("Start populating SD card data into mysql at "+site_name)
    files = os.listdir("/media/raid0/zeshi/AR_db/sd_data/"+site_name) # Need to change the folder name in real application
    if ".DS_Store" in files:
        files.remove(".DS_Store")
    site_id_query = ("SELECT site_id FROM sites WHERE site_name = %s")
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    cursor.execute(site_id_query, (site_name, ))
    site_id = cursor.fetchall()[0][0]
    if cursor.rowcount == 0:
        print("No site was found to site name:" + site_name)
        cursor.close()
        cnx.close()
        return
    node_query = ("SELECT sd_last_update FROM motes WHERE site_id = %s AND node_id = %s")
    update_table_motes = ("UPDATE motes SET sd_last_update = %s WHERE site_id = %s AND node_id = %s")
    insert_table_level_0 = ("INSERT INTO level_0 "
                            "(site_id, node_id, datetime, voltage, temperature, relative_humidity, "
                            "soil_moisture_1, soil_temperature_1, soil_ec_1, soil_moisture_2, "
                            "soil_temperature_2, soil_ec_2, snowdepth, judd_temp, unname_1, unname_2, "
                            "solar, maxibotics, sd_card) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    for f in files:
        # parse the file_name
        node_num_date = f[(len(site_name)+1):].split("_")
        node_id = int(node_num_date[0])
        date_str = node_num_date[1][:-4]
        node_visit_time = datetime.strptime(date_str, "%Y-%m-%d")
        try:
            cursor.execute(node_query, (site_id, node_id))
        except mysql.connector.Error as err:
            print("Querying error happens when trying to query time from node id " +                   str(node_id) + " at site " + site_name)
            print(err)
            continue
        sd_last_update = cursor.fetchall()[0][0]
        
        if cursor.rowcount == 0:
            print("No node found to node id: " + str(node_id) + " at site " + site_name)
            continue
        
        # If the node_visit_time is earlier than sd_last_update we continue over this file
        if sd_last_update and (node_visit_time < sd_last_update):
            continue
        
        fn = "/media/raid0/zeshi/AR_db/sd_data/" + site_name + "/" + f
        new_items = parse_sd_file(fn, site_id, node_id, sd_last_update)
        if new_items:
            update_data = new_items[0]
            new_sd_last_update = new_items[1]
            temp_temp = None
            try:
                cursor.execute(update_table_motes, (new_sd_last_update, site_id, node_id))
                cursor.executemany(insert_table_level_0, update_data)
                cnx.commit()
                print("Finished populating data from "+fn)
            except mysql.connector.Error as err:
                print("Error happens when inserting data!")
                print(temp_temp)
                print(err)
                break
    cursor.close()
    cnx.close()


# In[38]:

def site_info_check(site_name_id, node_id):
    """
    Check site id and node id valid from the database
    :param site_name_id:        int or string, The site name or site id of the data
    :param node_id:             int, node id
    """
    use_id = False
    if isinstance(site_name_id, int):
        site_id = site_name_id
        use_id = True
    if isinstance(site_name_id, str):
        site_name = site_name_id

    # Define all queries in this database
    site_id_query = ("SELECT site_id, num_of_nodes FROM sites WHERE site_name = %s")
    site_num_of_nodes_query = ("SELECT num_of_nodes FROM sites WHERE site_id = %s")

    # Connect to the ar_data database
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    
    # Check if site_name is valid
    if not use_id:
        try:
            cursor.execute(site_id_query, (site_name, ))
        except mysql.connector.Error as err:
            print(err)
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            cursor.close()
            cnx.close()
            raise ValueError("site name does not represent a valid site.")

        # Check if node_id is valid
        site_id = rows[0][0]
        max_num_nodes = rows[0][1]
        if node_id > max_num_nodes or node_id <= 0:
            cursor.close()
            cnx.close()
            raise ValueError("node_id does not represent a valid node in this site.")

    else:
        try:
            cursor.execute(site_num_of_nodes_query, (site_id, ))
        except mysql.connector.Error as err:
            print(err)
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            cursor.close()
            cnx.close()
            raise ValueError("site id does not represent a valid id.")

        max_num_nodes = rows[0][0]
        if node_id > max_num_nodes or node_id <= 0:
            cursor.close()
            cnx.close()
            raise ValueError("node_id does not represent a valid node in this site.")
    cursor.close()
    cnx.close()
    return site_id


# __The function below allows programmer to query data from the database by__
# ```
# site_name: string. The name of the site you want to query [Please look up the correct name]
# node_id: integer. The id of the node in that site
# starting_date: date.
# ending_date: date.
# field: string. The name of the column name
# ```

# In[39]:

def query_data_level0(site_name_id, node_id, starting_datetime, ending_datetime, field = None):
    """
    Query level0 data from mysql database
    :param site_name_id:        int or string, The site name or site id of the data
    :param node_id:             int, node id
    :param starting_datetime:   datetime, starting datetime of query
    :param ending_datetime:     datetime, ending datetime of query
    :param field:               string, name of the field to be queried
    :return:                    tuple, data rows that queried from the database
    """
    # Check if site_name_id and node_id valid
    try:
        site_id = site_info_check(site_name_id, node_id)
    except ValueError as err:
        print("Could not query data from level_0 table because of wrong site name/id or node id!")
        return None
    
    level0_column_name_query = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS" +
                                " WHERE TABLE_NAME='level_0'")

    # Check if field is specified
    if field is None:
        level0_data_query = ("SELECT * FROM level_0 WHERE site_id = %s AND node_id = %s "
                             "AND datetime >= %s AND datetime <= %s")
    else:
        query_string = "SELECT " + field + " FROM level_0 WHERE site_id = %s and node_id = %s " +                       "AND datetime >= %s AND datetime <= %s"
        level0_data_query = (query_string)

    # Connect to the ar_data database
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()

    # Check if fieldname is valid
    try:
        cursor.execute(level0_column_name_query)
    except mysql.connector.Error as err:
        print(err)
    rows = cursor.fetchall()
    rows = [item[0] for item in rows]
    if field is not None and field not in rows:
        cursor.close()
        cnx.close()
        raise ValueError("field is not a valid column in the table.")

    # Formatting start time
    try:
        cursor.execute(level0_data_query, (site_id, node_id, starting_datetime, ending_datetime))
    except mysql.connector.Error as err:
        print(level0_data_query)
        print(err)
    rows = cursor.fetchall()

    # Close the cursor and connector
    cursor.close()
    cnx.close()
    return rows

