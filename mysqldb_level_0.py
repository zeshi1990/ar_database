from __future__ import print_function
__author__ = "zeshi"

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import numpy as np
from datetime import datetime, date
import os


def init_db():
    """
    This function only runs once, just to initialize the database
    :return: None
    """
    print("Initialize database if not built before.")
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
    print("Starting building site table and populating data")
    site_info = pd.read_csv("server_data/site_node_info.csv", header=0, sep=",")
    site_names_all = site_info["site_id"].as_matrix()
    site_names = np.unique(site_info["site_id"].as_matrix())
    sites = ()
    for site_name in site_names:
        num_nodes = len(site_names_all[site_names_all == site_name])
        temp_site = (site_name, num_nodes)
        sites = sites + (temp_site,)

    cnx = mysql.connector.connect(user='root', password='root', database="ar_data")
    cursor = cnx.cursor()
    try:
        cursor.execute("ALTER TABLE sites AUTO_INCREMENT = 1")
        add_site = ("INSERT INTO sites "
                    "(site_name, num_of_nodes) "
                    "VALUES (%s, %s)")
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
        "  `temperature_temprh` float,"
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


def parse_server_file(fn, site_id, node_id, server_last_update):
    """
    This function parse the data file that received by the webserver from the base station
    :param fn:                  string, filename
    :param site_id:             int, site id, which get from the site id-site name mapping
    :param node_id:             int, node id
    :param server_last_update:  datetime, the last time server file being updated
    :return:                    tuple and datetime, parsed data and latest update timestamp
    """
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
                                        int(time_str[0]), int(time_str[1], int(time_str[2])))
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


def populate_data_server(site_name):
    """
    This function update the data received by the webserver
    :param site_name:           string, site name, format [Alpha, Bear_Trap, Caples_Lk, Duncan_Pk]
    :return:                    None
    """
    print("Start populating server data into mysql at "+site_name)
    try:
        files = os.listdir("server_data/"+site_name)
    except OSError as err:
        print("The site_name: " + site_name + " is not a valid data folder name!")
        return
    if len(files) == 0:
        print("The folder for this site_name: " + site_name + " is an empty folder!")
        return
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
            print("No node found to mac address: " + mac + site_name)
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
        fn = "server_data/" + site_name + "/" + f
        new_items = parse_server_file(fn, site_id, node_id, server_last_update)
        if new_items:
            update_data = new_items[0]
            new_server_last_update = new_items[1]
            try:
                cursor.execute(update_table_motes, (new_server_last_update, site_id, node_id, mac))
                if site_name != "caples_lk":
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


def parse_sd_file(fn, site_id, node_id, sd_last_update):
    """
    This function parse the data file that uploaded by Patrick downloaded from the sd cards
    :param fn:                  string, file name of the sd card data file
    :param site_id:             int, site id
    :param node_id:             int, node id
    :param sd_last_update:      datetime, last timestamp sd card updated
    :return:                    tuple and datetime, parsed sd card data and last update timestamp
    """
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
        # Only use the rows whose length is larger or equal to 10
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
                                    int(time_str[0]), int(time_str[1], int(time_str[2])))
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
                    if float(line_list[idx]) == -99.:
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


def populate_data_sd(site_name):
    """
    This function update the data uploaded by Patrick downloaded from the sd card
    :param site_name:           string, site name, format [Alpha, Bear_Trap, Caples_Lk, Duncan_Pk]
    :return:                    None
    """
    print("Start populating SD card data into mysql at "+site_name)
    try:
        files = os.listdir("sd_data/"+site_name) # Need to change the folder name in real application
    except OSError as err:
        print("The site_name: " + site_name + " is not a valid data folder name!")
        return
    if len(files) == 0:
        print("The folder for this site_name: " + site_name + " is an empty folder!")
        return
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
            print("Querying error happens when trying to query time from node id " + \
                  str(node_id) + " at site " + site_name)
            print(err)
            continue
        sd_last_update = cursor.fetchall()[0][0]

        if cursor.rowcount == 0:
            print("No node found to node id: " + str(node_id) + " at site " + site_name)
            continue

        # If the node_visit_time is earlier than sd_last_update we continue over this file
        if sd_last_update and (node_visit_time < sd_last_update):
            continue

        fn = "sd_data/" + site_name + "/" + f
        new_items = parse_sd_file(fn, site_id, node_id, sd_last_update)
        if new_items:
            update_data = new_items[0]
            new_sd_last_update = new_items[1]
            temp_temp = None
            try:
                cursor.execute(update_table_motes, (new_sd_last_update, site_id, node_id))
                cursor.executemany(insert_table_level_0, update_data)
                cnx.commit()
            except mysql.connector.Error as err:
                print("Error happens when inserting data!")
                print(temp_temp)
                print(err)
                break
    cursor.close()
    cnx.close()

def query_data_level0(site_name_id, node_id, starting_date, ending_date, field = None):
    """
    Query level0 data from mysql database
    :param site_name_id:        int or string, The site name or site id of the data
    :param node_id:             int, node id
    :param starting_date:       date, starting date of query
    :param ending_date:         date, ending date of query
    :param field:               string, name of the field to be queried
    :return:                    tuple, data rows that queried from the database
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
    level0_column_name_query = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS" +
                                " WHERE TABLE_NAME='level_0'")

    # Check if field is specified
    if field is None:
        level0_data_query = ("SELECT * FROM level_0 WHERE site_id = %s AND node_id = %s "
                             "AND datetime >= %s AND datetime <= %s")
    else:
        query_string = "SELECT " + field + " FROM level_0 WHERE site_id = %s and node_id = %s " +\
                       "AND datetime >= %s AND datetime <= %s"
        level0_data_query = (query_string)

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
            raise ValueError("site name does not represent a valid site.")

        # Check if node_id is valid
        site_id = rows[0][0]
        max_num_nodes = rows[0][1]
        if node_id > max_num_nodes or node_id <= 0:
            raise ValueError("node_id does not represent a valid node in this site.")

    else:
        try:
            cursor.execute(site_num_of_nodes_query, (site_id, ))
        except mysql.connector.Error as err:
            print(err)
        rows = cursor.fetchall()
        if cursor.rowcount == 0:
            raise ValueError("site id does not represent a valid id.")
        max_num_nodes = rows[0][0]
        if node_id > max_num_nodes or node_id <= 0:
            raise ValueError("node_id does not represent a valid node in this site.")

    # Check if fieldname is valid
    try:
        cursor.execute(level0_column_name_query)
    except mysql.connector.Error as err:
        print(err)
    rows = cursor.fetchall()
    rows = [item[0] for item in rows]
    if field is not None and field not in rows:
        raise ValueError("field is not a valid column in the table.")

    # Formatting start time
    starting_datetime = datetime.combine(starting_date, datetime.min.time())
    ending_datetime = datetime.combine(ending_date, datetime.max.time())
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

def snowdepth_baseline_update(WY):
    all_motes_query = ("SELECT site_id, node_id FROM motes")
    site_name_query = ("SELECT site_name FROM sites WHERE site_id = %s")
    baseline_update = ("UPDATE motes SET ground_dist = %s WHERE site_id = %s AND node_id = %s")
    min_datetime = datetime(WY-1, 8, 1)
    max_datetime = datetime(WY-1, 10, 1)
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    try:
        cursor.execute(all_motes_query)
    except mysql.connector.Error as err:
        print("Query motes problem")
        print(err)
    all_motes = cursor.fetchall()
    for mote in all_motes:
        mote_ground_distance = query_data_level0(mote[0], mote[1], min_datetime, max_datetime,
                                                 field='snowdepth')
        mote_ground_distance = np.array([i[0] for i in mote_ground_distance])
        mote_ground_distance = mote_ground_distance[mote_ground_distance >= 2000.]
        ground_distance = np.nanmean(mote_ground_distance)
        if mote_ground_distance.size == 0:
            ground_distance = None
        if ground_distance is not None:
            try:
                cursor.execute(baseline_update, (float(ground_distance), mote[0], mote[1]))
            except mysql.connector.Error as err:
                print(err)
    cnx.commit()
    cursor.close()
    cnx.close()