"""
This file takes a comma delimited csv file of transactions as an input, and computes the fastest
growing items.

Use: python fastest_growing_items.py /path/to/transaction/file.csv

TODO:
- Aggregate and find trends for week, month, year, etc.
- Make function to plot graphs and trend lines
- Figure out how to scale (parallelization, MapReduce, etc.)

Author: Nathaniel M. Burley
"""
import sys, os, csv
import heapq
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

target_upc = 4011

######################################## PRE-PROCESSING ############################################

# GET OUR COMMAND LINE ARGUEMENTS
datafile_paths = sys.argv[1:len(sys.argv)]
print("Reading in files {}...".format(datafile_paths))


# Function that reads all datafiles into a pandas dataframe (sorted by ascending transaction IDs)
def buildDataFrame(infile_list):
    df = pd.concat([pd.read_csv(f) for f in infile_list], ignore_index = True)
    df = pd.concat([df.iloc[:,1:3], df.iloc[:,7:8], df.iloc[:,13:16]], axis=1)
    df = df.sort_values('TRANSACTION_ID')
    df['UPC'] = df['UPC'].astype('int64')
    df['Quantity'] = df['Quantity'].astype('int64')
    df['TRANSACTION_ID'] = df['TRANSACTION_ID'].astype('int64')
    df["Date"] = df["Date"].astype("datetime64")
    return df


# Function that turns the date row into number of days since the earliest date
def convertDate(earliest_date, row_date):
    num_days = row_date - earliest_date
    #print(num_days.days)
    return num_days.days


######################################## READ IN DATA ##############################################

# Data read into data file
transaction_df = buildDataFrame(datafile_paths)

################################### CALCULATE FASTEST GROWING PRODUCTS #############################
# - Get list of unique UPCs 
# - For each UPC: Filter rows by that, Count sales per week, Figure out trend (regression line slope)
# - To figure out the regression line: 
    # X values: Number of days since the first sale
    # Y values: Number of sales on that day

# Gather a list of unique UPCs
items = transaction_df['UPC'].unique()

# Make a dataframe for each UPC
for upc in items:
    is_target = transaction_df['UPC'] == upc
    og_upc_df = transaction_df[is_target]    #target_upc_df = og_upc_df.groupby('Date')
    num_unique_dates = len(og_upc_df.groupby('Date')['Date'].unique())
    
    # Now that we have entries with enough dates, we will calculate the regression slope for each
    if num_unique_dates > 7:
        # X values computed here
        earliest_date = og_upc_df['Date'].min()
        og_upc_df['X'] = og_upc_df.apply(lambda x: convertDate(earliest_date, x['Date']), axis=1)
        #print(og_upc_df.groupby('Date').head(n=10))

        # Regression coefficient computed here (least squares method, link below)
        #(https://stattrek.com/multiple-regression/regression-coefficients.aspx)
        #og_upc_df.plot(x='X', y='Quantity', style='o')
        #plt.show(block=True)
        x_mean = og_upc_df['X'].mean()
        y_mean = og_upc_df['Quantity'].mean()
        og_upc_df['X_Min_Mean'] = og_upc_df['X'] - x_mean
        og_upc_df['X_Min_Mean_Sqrd'] = (og_upc_df['X'] - x_mean) ** 2
        og_upc_df['Y_Min_Mean'] = og_upc_df['Quantity'] - y_mean
        og_upc_df['(Xi-X)(Yi-Y)'] = og_upc_df['X_Min_Mean'] * og_upc_df['Y_Min_Mean']
        reg_coef = sum(og_upc_df['(Xi-X)(Yi-Y)']) / sum(og_upc_df['X_Min_Mean_Sqrd'])
        print("Growth rate for {}: {}".format(upc, reg_coef))

        """
        # CODE FOR OLD FILE FORMAT (NO QUANTITY COLUMN)
        # Y values computed here
        reg_vals = pd.DataFrame()
        reg_vals['X'] = og_upc_df['X_Vals'].unique()
        #reg_vals['Y'] = reg_vals['X'].map(og_upc_df['X_Vals'].value_counts())
        reg_vals['Y'] = og_upc_df['Quantity']
        #print(reg_vals.head(n=20))
    
        # Regression coefficient computed here (least squares method, link below)
        #(https://stattrek.com/multiple-regression/regression-coefficients.aspx)
        x_mean = reg_vals['X'].mean()
        y_mean = reg_vals['Y'].mean()
        reg_vals['X_Min_Mean'] = reg_vals['X'] - x_mean
        reg_vals['X_Min_Mean_Sqrd'] = (reg_vals['X'] - x_mean) ** 2
        reg_vals['Y_Min_Mean'] = reg_vals['Y'] - y_mean
        reg_vals['(Xi-X)(Yi-Y)'] = reg_vals['X_Min_Mean'] * reg_vals['Y_Min_Mean']
        reg_coef = sum(reg_vals['(Xi-X)(Yi-Y)']) / sum(reg_vals['X_Min_Mean_Sqrd'])
        #print(reg_vals.head(n=5))
        #print("Growth rate for {}: {}".format(upc, reg_coef))
        """
