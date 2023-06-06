#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 13:47:49 2023

@author: caelelmore
"""
import pandas as pd
from datetime import datetime

filename = "out.tsv"
pd.options.mode.copy_on_write = True

def aggregate(filename):
    # Read the TSV file and create the initial dataframe
    df = pd.read_csv('out.tsv', sep='\t')
    
    # Convert 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Create a column that tells whether it is a day or week summary
    df['type'] = 'day'
    
    # Sort the dataframe by 'date' column
    df = df.sort_values('date')
    
    # Rearrange the order of the columns
    column_order = ['hcw', 'type', 'nday', 'nweek', 'date', 'days_worked', 'jtid', 'visits', 'rooms', 'stime', 'etime', 'time_worked']
    df = df.reindex(columns=column_order)   
    
    # Create a new dataframe to store the summary rows
    summary_rows = []
    
    # Iterate over unique healthcare workers
    for hcw, hcw_df in df.groupby('hcw'):
        # Create a dictionary of values for the total summary per hcw
        hcw_summary_values = {
            'date': hcw_df['date'].min(),
            'hcw': hcw,
            'days_worked': hcw_df['days_worked'].sum(),
            'visits': hcw_df['visits'].sum(),
            'rooms': hcw_df['rooms'].sum(),
            'stime': hcw_df['stime'].min(),
            'etime': hcw_df['etime'].max(),
            'time_worked': time_diff(hcw_df['stime'].min(), hcw_df['etime'].max()),
            'type': 'all'
        }
        
        hcw_summary_row = pd.DataFrame([hcw_summary_values], columns = df.columns)
        
        # Iterate over unique weeks for the current healthcare worker
        for week, week_df in hcw_df.groupby('nweek'):    
           
            formatted_diff = time_diff(week_df['stime'].min(), week_df['etime'].max())
            # Calculate summary values for the week and healthcare worker
            summary_values = {
                'date': week_df['date'].min(),
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
        
        # Add the total hcw summary at the bottom of each hcw's data
        summary_rows.append(hcw_summary_row)
        
    # Concatenate all the summary rows into a single dataframe
    summary_df = pd.concat(summary_rows, ignore_index=True)
    
    # Reset the index of the summary dataframe
    summary_df.reset_index(drop=True, inplace=True)
    
    # Print the resulting dataframe
    print(summary_df.head(60))
    
    # Export the dataframe to a csv
    summary_df.to_csv("Staff_Summary.csv")
   
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

aggregate(filename)
