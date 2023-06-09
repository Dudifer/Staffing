#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 09:25:12 2023

@author: caelelmore

To transfer python file to vinci:
    go to the proper directory from your terminal
    for me, that is cd desktop, cd uiowa
    then, scp filename.py hawkid@vinci.cs.uiowa.edu:filename.py
    same thing for the config file
    
    then, to run the python file with the config file as input,
    go to vinci
    then, use this command:
        python filename.py config.cfg
    then it should run properly
"""

import re
from dateutil.parser import parse
from datetime import datetime
import pymysql
import pandas as pd
import sys

pd.options.mode.copy_on_write = True
select = "SELECT MOD(DAYOFWEEK(v.itime)+5, 7) AS nday, DATE(v.itime) AS date, WEEKOFYEAR(v.itime) AS nweek, v.hid AS hcw, r.uid, COUNT(DISTINCT DATE(v.itime)) AS days_worked, j.jtid, COUNT(v.vid) AS visits, COUNT(DISTINCT v.rid) AS rooms, MIN(TIME(v.itime)) AS stime, MAX(TIME(v.otime)) AS etime, TIMEDIFF(MAX(TIME(v.otime)), MIN(TIME(v.itime))) as time_worked "
from1 = " FROM visits v, hcws h, jobs j, rooms r "
where = " WHERE v.hid=h.hid AND h.jid=j.jid AND v.rid=r.rid "
group_by = " GROUP BY v.hid, date;"

filename = sys.argv[1]

with open(filename, "r") as config:
    for line in config:
        line = line.strip().split(":")
        match line[0]:
            case "shift":
                where += " AND v.shift = '" + line[1].replace(" ", "") +"'"
            case "hid":
                hid_list = []
                for a, b in re.findall(r'(\d+)-?(\d*)', str(line[1:])):
                    hid_list.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
                where += " AND v.hid in ("
                for item in hid_list:
                    where += str(item) + ","
                where = where[:-1] + ") "
            case "uid":
                where += " AND r.uid in ("
                uid_list = []
                for a, b in re.findall(r'(\d+)-?(\d*)', str(line[1:])):
                    uid_list.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
                for item in uid_list:
                    where += str(item) + ","
                where = where[:-1] + ") "
            case "did":
                where += " AND h.did in ("
                did_list = []
                for a, b in re.findall(r'(\d+)-?(\d*)', str(line[1:])):
                    did_list.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
                for item in did_list:
                    where += str(item) + ","
                where = where[:-1] + ") "
            case "jtid":
                where += " AND j.jtid in ("
                jtid_list = []
                for a, b in re.findall(r'(\d+)-?(\d*)', str(line[1:])):
                    jtid_list.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
                for item in jtid_list:
                    where += str(item) + ","
                where = where[:-1] + ") "
            case "ftid":
                from1 += ", facilities f"
                where += " AND h.fid = f.fid "
                where += " AND f.ftid in ("
                ftid_list = []
                for a, b in re.findall(r'(\d+)-?(\d*)', str(line[1:])):
                    ftid_list.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
                for item in ftid_list:
                    where += str(item) + ","
                where = where[:-1] + ") "
            case "itime":
                itime = ":".join(line[1:])
                itime.replace("/", "-")
                itime = parse(itime)
                itime = str(itime.date()) + " " + str(itime.time())
                if len(where) == 0:
                    where += "WHERE v.itime > '" + itime + "' "
                else: 
                    where += " AND v.itime > '" + itime + "' "
            case "otime":
                otime = ":".join(line[1:])
                otime.replace("/", "-")
                otime = parse(otime)
                otime = str(otime.date()) + " " + str(otime.time())
                if len(where) == 0:
                    where += "WHERE v.otime < '" + otime + "' "
                else: 
                    where += " AND v.otime < '" + otime + "' "
                
my_query = select + " " + from1 + " " + where + " " + group_by

print(my_query)

connection = pymysql.connect(host="localhost", user='celmor', database='ssense', password='celmor')
df = pd.read_sql(my_query, connection)


def aggregate(df):

    df['date'] = pd.to_datetime(df['date'])
    
    # Create a year column so we can group by year as well
    df['year'] = df['date'].dt.year
        
    # Create a column that tells whether it is a day or week summary
    df['type'] = 'day'
    
    # Change stime and etime into strings
    df['stime'] = df['stime'].apply(lambda x: str(x))
    df['etime'] =  df['etime'].apply(lambda x: str(x))
    df['time_worked'] =  df['time_worked'].apply(lambda x: str(x))
    
    # Get stime, etime, and time_worked in the right format HH:MM:SS
    df['stime'] = df['stime'].apply(extract_time)
    df['etime'] = df['etime'].apply(extract_time)
    df['time_worked'] = df['time_worked'].apply(extract_time)

    # Sort the dataframe by 'date' column
    df = df.sort_values('date')
    
    # Rearrange the order of the columns
    column_order = ['hcw', 'type', 'nday', 'nweek', 'year', 'date', 'days_worked', 'jtid', 'visits', 'rooms', 'stime', 'etime', 'time_worked']
    df = df.reindex(columns=column_order)   
    
    
    # Create a new dataframe to store the summary rows
    summary_rows = []
    
    
    # Iterate over unique healthcare workers
    for hcw, hcw_df in df.groupby('hcw'):
        # Create a dictionary of values for the total summary per hcw
        hcw_summary_values = {
            'hcw': hcw,
            'days_worked': hcw_df['days_worked'].sum(),
            'visits': hcw_df['visits'].sum(),
            'rooms': hcw_df['rooms'].sum(),
            'stime': hcw_df['stime'].apply(extract_time).min(),
            'etime': hcw_df['etime'].apply(extract_time).max(),
            'time_worked': time_diff(str(hcw_df['stime'].min()), str(hcw_df['etime'].max())),
            'type': 'all'
        }
        
        hcw_summary_row = pd.DataFrame([hcw_summary_values], columns = df.columns)
        
        # Iterate over unique weeks for the current healthcare worker
        for (year, week), week_df in hcw_df.groupby(['year', 'nweek']):    
           
            formatted_diff = time_diff(str(week_df['stime'].min()), str(week_df['etime'].max()))
            # Calculate summary values for the week and healthcare worker
            summary_values = {
                'nweek': week,
                'hcw': hcw,
                'days_worked': week_df['days_worked'].sum(),
                'visits': week_df['visits'].sum(),
                'rooms': week_df['rooms'].sum(),
                'stime': week_df['stime'].min(),
                'etime': week_df['etime'].max(),
                'time_worked': formatted_diff,
                'type': 'week'
            }
            
            # Create a summary row as a dataframe
            summary_row = pd.DataFrame([summary_values], columns=df.columns)
            
            # Append the original week dataframe
            week_data = pd.concat([week_df, summary_row], ignore_index=True)
            
            # Append the week's data to the list of summary rows
            summary_rows.append(week_data)
        
        summary_rows.append(hcw_summary_row)
    # Concatenate all the summary rows into a single dataframe
    summary_df = pd.concat(summary_rows, ignore_index=True)
    
    # Reset the index of the summary dataframe
    summary_df.reset_index(drop=True, inplace=True)
    
    # Print the resulting dataframe
    print(summary_df)
    
    # Print the shape of the dataframe
    print(summary_df.shape)
    
    # Export the dataframe to a csv
    summary_df.to_csv("staff_summary.tsv", sep = "\t")
    
    
   
def time_diff (t1, t2):

    # Convert times to datetime objects
    format_string = '%H:%M:%S'
    dt1 = datetime.strptime(t1, format_string)
    dt2 = datetime.strptime(t2, format_string)

    # Subtract the datetime objects
    time_diff = dt2 - dt1

    # Calculate the total seconds in the time difference
    total_seconds = time_diff.total_seconds()

    # Calculate the hours, minutes, and seconds from total seconds
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)

    # Format the time difference
    formatted_diff = f'{hours:02d}:{minutes:02d}:{seconds:02d}'
    
    return formatted_diff


def extract_time(string):
    time_pattern = r"(\d{2}:\d{2}:\d{2})"
    matches = re.findall(time_pattern, string)
    if matches:
        return matches[0]
    else:
        return None

aggregate(df)