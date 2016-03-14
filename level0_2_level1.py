
# coding: utf-8

# In[1]:

from __future__ import print_function
__author__ = "zeshi"

import numpy as np
from mysqldb_level_0 import query_data_level0, site_info_check
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, date, timedelta


# Define all querie in this database
site_id_query = ("SELECT site_id, num_of_nodes FROM sites WHERE site_name = %s")
level1_time_query = ("SELECT sd_level_1, server_level_1 FROM motes WHERE site_id = %s AND node_id = %s")
level0_time_query = ("SELECT sd_last_update, server_last_update FROM motes WHERE site_id = %s " 
                     "AND node_id = %s")
level1_insert_string = ("INSERT INTO level_1 "
                        "(site_id, node_id, datetime, voltage, temperature, relative_humidity, "
                        "soil_moisture_1, soil_temperature_1, soil_ec_1, soil_moisture_2, "
                        "soil_temperature_2, soil_ec_2, snowdepth, judd_temp, solar, maxibotics, sd_card) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
level1_sd_time_update = ("UPDATE motes SET sd_level_1 = %s WHERE site_id = %s AND node_id = %s")
level1_server_time_update = ("UPDATE motes SET server_level_1 = %s WHERE site_id = %s AND node_id = %s")


# In[2]:

def formater(data_row):
    output = ()
    for i, item in enumerate(data_row):
        if i != 14 and i != 15:
            output = output + (item, )
    return output


# In[3]:

def query_site_id(site_name, cursor):
    try:
        cursor.execute(site_id_query, (site_name, ))
        site_id = cursor.fetchall()[0][0]
        return site_id
    except mysql.connector.Error as err:
        print(err)
        return None
    except IndexError as err:
        print(err)
        return None


# In[4]:

def query_time(site_id, node_id, time_query_string, cursor):
    try:
        cursor.execute(time_query_string, (site_id, node_id))
        time = cursor.fetchall()[0]
        return time
    except IndexError as err:
        print("The node_id is wrong!")
        return (None, None)


# In[5]:

def level0_to_level1_time(site_name, node_id):
    """
    This function return the maximum time range to update the level1 table from level0 table
    :param: site_name:          string, site name
    :param node_id:             int, node id
    """
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    site_id = query_site_id(site_name, cursor)
    if site_id is None:
        cursor.close()
        cnx.close()
        print(site_name + "is not a valid site name!")
        return (None, None, None)
    sd_last_update, server_last_update = query_time(site_id, node_id, level0_time_query, cursor)
    if sd_last_update is None and server_last_update is None:
        print(site_name + ": node_" + str(node_id) + " has not been updated for level_0 data yet " + 
              "or node_id is wrong")
        cursor.close()
        cnx.close()
        return (None, None, None)
    else:
        if sd_last_update is None:
            ending_datetime = server_last_update
        elif server_last_update is None:
            ending_datetime = sd_last_update
        else:
            if sd_last_update > server_last_update:
                ending_datetime = sd_last_update
            else:
                ending_datetime = server_last_update
    sd_level_1, server_level_1 = query_time(site_id, node_id, level1_time_query, cursor)
    if sd_level_1 > server_level_1:
        starting_datetime = server_level_1
    else:
        starting_datetime = sd_level_1
    cursor.close()
    cnx.close()
    return (site_id, starting_datetime, ending_datetime)


# In[6]:

def update_data_level1(site_name_id, node_id, row_datetime, new_row):
    """
    Update level1 data from mysql database
    :param site_name_id:        int or string, The site name or site id of the data
    :param node_id:             int, node id
    :param row_datetime         datetime, the datetime of the row we are going to update
    :param new_row              tuple, the new data row to replace the old row
    """
    # Check if site_name_id and node_id valid
    try:
        site_id = site_info_check(site_name_id, node_id)
    except ValueError as err:
        print("Could not update row because of invalid site name/id and node id!")
        return
        
    # Define query string
    level1_update_query = ("UPDATE level_1 SET voltage = %s, "
                           "temperature = %s, relative_humidity = %s, soil_moisture_1 = %s, "
                           "soil_temperature_1 = %s, soil_ec_1 = %s, soil_moisture_2 = %s, soil_temperature_2 = %s, "
                           "soil_ec_2 = %s, snowdepth = %s, judd_temp = %s, solar = %s, "
                           "maxibotics = %s, sd_card = %s WHERE site_id = %s AND node_id = %s AND datetime = %s")
    exec_data = ()
    for i in range(3, 19):
        if i != 14 and i != 15:
            exec_data = exec_data + (new_row[i], )
    for i in range(0, 3):
        exec_data = exec_data + (new_row[i], )
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    try:
        cursor.execute(level1_update_query, exec_data)
        cnx.commit()
    except mysql.connector.Error as err:
        print("Update failed because of mysql error!")
        print(err)
    cursor.close()
    cnx.close()


# In[7]:

def query_data_level1(site_name_id, node_id, starting_datetime, ending_datetime, field = None):
    """
    Query level1 data from mysql database
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
        print("Could not query data from level_1 table because of wrong site name/id or node id!")
        return None
    
    # Define all queries in this database
    level0_column_name_query = ("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS" +
                                " WHERE TABLE_NAME='level_1'")

    # Check if field is specified
    if field is None:
        level1_data_query = ("SELECT * FROM level_1 WHERE site_id = %s AND node_id = %s "
                             "AND datetime >= %s AND datetime <= %s")
    else:
        query_string = "SELECT " + field + " FROM level_1 WHERE site_id = %s and node_id = %s " + "AND datetime >= %s AND datetime <= %s"
        level1_data_query = (query_string)

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

    # Start querying data points
    try:
        cursor.execute(level1_data_query, (site_id, node_id, starting_datetime, ending_datetime))
    except mysql.connector.Error as err:
        print("Level_1 data query failed!")
        print(err)
    rows = cursor.fetchall()

    # Close the cursor and connector
    cursor.close()
    cnx.close()
    return rows


# In[15]:

def level0_to_level1_data_merge(site_name, node_id, datetime_range_interupt = None):
    site_id, starting_datetime, ending_datetime = level0_to_level1_time(site_name, node_id) 
    if site_id is None and starting_datetime is None and ending_datetime is None:
        return
    if datetime_range_interupt is not None:
        starting_datetime = datetime_range_interupt[0]
        ending_datetime = datetime_range_interupt[1]
    datetime_list = []
    temp = starting_datetime
    new_sd_level_1 = None
    new_server_level_1 = None
    while temp <= ending_datetime:
        datetime_list.append(temp)
        temp += timedelta(minutes = 15)
    output = ()
    for temp_datetime in datetime_list:
        level_0_data_temp = query_data_level0(site_id, node_id, temp_datetime, temp_datetime)
        level_0_data_temp_length = len(level_0_data_temp)
        level_1_data_temp = query_data_level1(site_id, node_id, temp_datetime, temp_datetime)
        level_1_data_temp_length = len(level_1_data_temp)
        if level_0_data_temp_length == 0:
            if level_1_data_temp_length == 1:
                continue
            elif level_1_data_temp_length == 0:
                output = output + ((site_id, node_id, temp_datetime, None, None, None, None, 
                                    None, None, None, None, None, None, None, None, None, None), )
        elif level_0_data_temp_length == 1:
            if level_1_data_temp_length == 1:
                update_data_level1(site_id, node_id, temp_datetime, level_0_data_temp[0])
            elif level_1_data_temp_length == 0:
                output = output + (formater(level_0_data_temp[0]), )
            if level_0_data_temp[0][-1] == 0:
                new_server_level_1 = temp_datetime
            else:
                new_sd_level_1 = temp_datetime
        elif level_0_data_temp_length >= 2:
            if level_1_data_temp_length == 1:
                updated = False
                for i in range(0, level_0_data_temp_length): 
                    if level_0_data_temp[i][-1] == 1:
                        update_data_level1(site_id, node_id, temp_datetime, level_0_data_temp[i])
                        updated = True
                        new_sd_level_1 = temp_datetime
                        break
                if not updated:
                    update_data_level1(site_id, node_id, temp_datetime, level_0_data_temp[0])
                    new_server_level_1 = temp_datetime
            elif level_1_data_temp_length == 0:
                inserted = False
                for i in range(0, level_0_data_temp_length): 
                    if level_0_data_temp[i][-1] == 1:
                        output = output + (formater(level_0_data_temp[i]), )
                        inserted = True
                        new_sd_level_1 = temp_datetime
                        break
                if not inserted:
                    output = output + (formater(level_0_data_temp[0]), )
                    new_server_level_1 = temp_datetime
    cnx = mysql.connector.connect(user='root', password='root', database='ar_data')
    cursor = cnx.cursor()
    if output == ():
        print("Level_1 data table updated from level_0 table!")
        if datetime_range_interupt is None:
            try:
                if new_server_level_1 is not None:
                    cursor.execute(level1_server_time_update, (new_server_level_1, site_id, node_id))
                if new_sd_level_1 is not None:
                    cursor.execute(level1_sd_time_update, (new_sd_level_1, site_id, node_id))
                cnx.commit()
            except mysql.connector.Error as err:
                print(err)
                print("Updating time error!")
        cursor.close()
        cnx.close()
        return
    else:
        try:
            cursor.executemany(level1_insert_string, output)
            cnx.commit()
            print("Level_1 data table updated from level_0 table!")
        except mysql.connector.Error as err:
            print(err)
            print("Inserting data into level_1 table failed.")
        if datetime_range_interupt is None:
            try:
                if new_server_level_1 is not None:
                    cursor.execute(level1_server_time_update, (new_server_level_1, site_id, node_id))
                if new_sd_level_1 is not None:
                    cursor.execute(level1_sd_time_update, (new_sd_level_1, site_id, node_id))
                cnx.commit()
            except mysql.connector.Error as err:
                print(err)
                print("Updating time error!")
        cursor.close()
        cnx.close()
        return


# In[17]:

# Testing cell
# for i in [1,2,3,4,5,6,7,8,11]:
#     level0_to_level1_data_merge("Duncan_Pk", i, datetime_range_interupt=(datetime(2016, 1, 25), datetime(2016, 2, 1)))

