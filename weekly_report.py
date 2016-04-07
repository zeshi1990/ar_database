#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.pooling import MySQLConnectionPool
from datetime import datetime, timedelta
import numpy as np
from matplotlib import pyplot as plt
import matplotlib as mpl
from level1_cleaning import level1_cleaning_site

mpl.rc("font", family="Helvetica")
mpl.rc("font", size=12)


# In[ ]:

def init():
    global cnx
    cnx = mysql.connector.connect(user="root", password="root", database="ar_data")
    site_info_query_command = "SELECT * FROM sites"
    site_table = pd.read_sql_query(site_info_query_command, cnx)
    return site_table


# In[ ]:

def init_pool():
    global pool
    dbconfig = {
        "database": "ar_data",
        "user": "root",
        "password": "root"
    }
    pool = MySQLConnectionPool(pool_name = "para_pool", pool_size = 10, **dbconfig)


# # Define week start datetime, this needs to be automated after this report

# In[ ]:

def node_info_query_by_site(site_table, start_time):
    valid_motes = np.empty((0, 4))
    for index, site_info in site_table.iterrows():
        site_id = site_info['site_id']
        site_name = site_info['site_name']
        node_info_query_command = "SELECT node_id, elevation, server_last_update, " +                                   " sd_last_update, ground_dist FROM motes WHERE site_id = " + str(site_id)
        node_table = pd.read_sql_query(node_info_query_command, cnx)
        for node_index, node_info in node_table.iterrows():
            node_id = node_info['node_id']
            elevation = node_info['elevation']
            server_last_update = node_info['server_last_update']
            sd_last_update = node_info['sd_last_update']
            ground_dist = node_info['ground_dist']
            if ground_dist is None:
                continue
            if server_last_update is None and sd_last_update is None:
                continue
            if np.isnan(ground_dist):
                continue
            if server_last_update is None and sd_last_update < start_time:
                continue
            if sd_last_update is None and server_last_update < start_time:
                continue
            if sd_last_update is not None and server_last_update is not None:
                if sd_last_update < start_time and server_last_update < start_time:
                    continue
            valid_motes = np.vstack((valid_motes, np.array([site_id, node_id, elevation, ground_dist])))
    return valid_motes


# In[ ]:

def clean_snowdepth_para(site_id, days=1, start_time=datetime.now()):
    print "Start cleaning snowdepth from site", site_id
    clean_cnx = pool.get_connection()
    for dt in range(0, days):
        temp_datetime = start_time + timedelta(days=dt)
        level1_cleaning_site(site_id, temp_datetime, temp_datetime + timedelta(days=1), clean_cnx)
    clean_cnx.close()
    print "Finished cleaning snowdepth from site", site_id


# In[ ]:

def clean_snowdepth(site_table, start_time, ending_time = None):            
    # For weekly snowdepth cleaning
    if ending_time is None:
        for index, site_info in site_table.iterrows():
            site_id = site_info['site_id']
            for dt in range(0, 7):
                temp_datetime = start_time + timedelta(days=dt)
                print site_id, temp_datetime, "start to clean"
                level1_cleaning_site(site_id, temp_datetime, temp_datetime + timedelta(days=1))
        return
    # For long term snowdepth cleaning
    else:
        days = (ending_time - start_time).days
        all_sites_id = []
        for index, site_info in site_table.iterrows():
            site_id = site_info['site_id']
            all_sites_id.append(site_id)
        clean_snowdepth_para_partial = partial(clean_snowdepth_para, 
                                               days=days, 
                                               start_time=start_time)
        pool_worker = Pool(processes=8, initializer=init_pool)
        pool_worker.map(clean_snowdepth_para_partial, all_sites_id)
        pool_worker.close()
        pool_worker.join()


# In[ ]:

def query_clean_snowdepth(valid_motes, week_start_time):
    week_end_time = week_start_time + timedelta(days=7)
    site_id_unique = np.unique(valid_motes[:, 0])
    site_sd_info = np.empty((0, 4))
    for site_id in site_id_unique:
        site_valid_motes = valid_motes[valid_motes[:, 0] == site_id]
        site_clean_sd = np.empty((1, 0))
        elevation_mean = np.nanmean(site_valid_motes[:, 2])
        for mote_info in site_valid_motes:
            site_id = int(mote_info[0])
            node_id = int(mote_info[1])
            elevation = int(mote_info[2])
            ground_dist = int(mote_info[3])
            query_string = "SELECT sd_clean FROM level_1 WHERE site_id = " + str(site_id) + " AND node_id = " + str(node_id) +                            " AND datetime <= '" + week_end_time.strftime("%Y-%m-%d %H:%M:%S") + "' AND datetime >= '" +                            week_start_time.strftime("%Y-%m-%d %H:%M:%S") + "'"
            clean_sd = pd.read_sql_query(query_string, cnx).as_matrix()
            if clean_sd[0, 0] is None:
                continue
            clean_sd = ground_dist - clean_sd
            site_clean_sd = np.append(site_clean_sd, clean_sd)
        sd_mean = np.nanmean(site_clean_sd)
        sd_std = np.nanstd(site_clean_sd)
        site_sd_info = np.vstack((site_sd_info, np.array([site_id, elevation_mean, sd_mean, sd_std])))
    return site_sd_info


# In[ ]:

def datetime_to_string(datetime_datetime):
    return "'" + datetime_datetime.strftime("%Y-%m-%d %H:%M:%S") + "'"


# In[ ]:

def weekly_avg_std_by_site(site_table, clean=False):
    week_start_time = datetime.now() - timedelta(days=7)
    year = week_start_time.year
    month = week_start_time.month
    day = week_start_time.day
    week_start_time = datetime(year, month, day)
    valid_motes = node_info_query_by_site(site_table, week_start_time)
    if clean:
        clean_snowdepth(site_table, week_start_time)
    site_sd_info = query_clean_snowdepth(valid_motes, week_start_time)
    site_sd_info = site_sd_info[~np.isnan(site_sd_info[:, 2])]
    site_sd_info = site_sd_info[np.argsort(site_sd_info[:, 1])]
    site_name = []
    for temp_site_sd_info in site_sd_info:
        site_name.append(site_table.loc[site_table['site_id'] == int(temp_site_sd_info[0]), 'site_name'].as_matrix()[0])
    x = np.array(range(1, len(site_sd_info)+1))
    sd = site_sd_info[:, 2] / 10.
    sd_std = site_sd_info[:, 3] / 10.
    plt.figure(figsize=(12, 8))
    plt.xticks(x, site_name)
    plt.errorbar(x, sd, yerr=sd_std, fmt='o')
    plt.xlim([0, len(site_name)+1])
    plt.xlabel("Site name, sorted by elevation")
    plt.ylabel("Snow depth, cm")
    plt.grid()
    fn = "/media/raid0/zeshi/AR_db/figures/avg_" + str(year)+str(month).zfill(2)+str(day).zfill(2)+".pdf"
    plt.savefig(fn)


# In[ ]:

def ts_query_by_site(site_motes, ts_start_time_str, ts_stop_time_str):
    sd_matrix = None
    datetime_matrix = None
    for mote in site_motes:
        site_id = int(mote[0])
        node_id = int(mote[1])
        elevation = int(mote[2])
        ground_dist = int(mote[3])
        query_str = "SELECT datetime, sd_clean FROM level_1 WHERE site_id = " + str(site_id) + " AND node_id = " + str(node_id) +                     " AND datetime <= " + ts_stop_time_str + " AND datetime >= " + ts_start_time_str + " ORDER BY datetime"
        clean_sd_datetime = pd.read_sql_query(query_str, cnx)
        clean_sd = clean_sd_datetime.loc[:, 'sd_clean'].as_matrix()
        datetime_arr = clean_sd_datetime.loc[:, 'datetime'].as_matrix()
        if clean_sd[0] is None:
            continue
        clean_sd = ground_dist - clean_sd
        if sd_matrix is None:
            sd_matrix = clean_sd
            datetime_matrix = datetime_arr
        else:
            sd_matrix = np.column_stack((sd_matrix, clean_sd))
    return sd_matrix, datetime_matrix


# In[ ]:

def ts_wyd_by_site(site_table, clean = False):
    now = datetime.now()
    current_month = now.month
    current_year = now.year
    if current_month >= 10:
        wy = current_year + 1
    else:
        wy = current_year
    ts_start_time = datetime(wy-1, 10, 1)
    ts_stop_time = datetime(now.year, now.month, now.day)
    # If cleaning is true, usually this should be False
    if clean:
        clean_snowdepth(site_table, ts_start_time, ts_stop_time) 
    ts_start_time_str = datetime_to_string(ts_start_time)
    ts_stop_time_str = datetime_to_string(ts_stop_time)
    valid_motes = node_info_query_by_site(site_table, ts_start_time)
    valid_motes = valid_motes[valid_motes[:, 3]!=9999.]
    unique_sites_id = np.unique(valid_motes[:, 0]).astype(int)
    fig, axarr = plt.subplots(len(unique_sites_id), sharex=True, figsize=(20, len(unique_sites_id) * 5))
    for i, site_id in enumerate(unique_sites_id):
        site_motes = valid_motes[valid_motes[:, 0] == site_id]
        site_sd_clean, site_datetime = ts_query_by_site(site_motes, ts_start_time_str, ts_stop_time_str)
        site_name = site_table.site_name[site_id-1]
        if site_sd_clean is None:
            continue
        ts_sd_avg = np.nanmean(site_sd_clean, axis=1)
        ts_sd_avg /= 10.
        ts_sd_std = np.nanstd(site_sd_clean, axis=1)
        ts_sd_std /= 10.
        data_length = len(ts_sd_std)
        axarr[i].plot(site_datetime, ts_sd_avg)
        axarr[i].fill_between(site_datetime, ts_sd_avg - ts_sd_std, ts_sd_avg + ts_sd_std, facecolor="grey", alpha=0.6)
        axarr[i].set_ylim([0, 300])
        axarr[i].text(datetime(wy-1, 10, 5), 250, site_name)
        axarr[i].grid()
    plt.xlabel("Time")
    fig.text(0.06, 0.5, 'Snowdepth, cm', ha='center', va='center', rotation='vertical')
    fn = "/media/raid0/zeshi/AR_db/figures/ts_" + str(year)+str(month).zfill(2)+str(day).zfill(2)+".pdf"
    plt.savefig(fn)


# In[ ]:

def week_report():
    site_table = init()
    weekly_avg_std_by_site(site_table)
    ts_wyd_by_site(site_table)
    cnx.close()


# In[ ]:

week_report()

