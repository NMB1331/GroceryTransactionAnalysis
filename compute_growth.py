"""
This file takes a comma delimited csv file of transactions as an input, and computes the growths
per week, month, and year for each UPC, and outputs a .csv file with the results of the calculations

Use: python compute_growth.py /path/to/transaction/file.csv

TODO:
- Figure out top selling UPCs for each quarter (use this logic to sum up quantity: practice_year_transactions = year_df_list[0].groupby(['UPC', pd.Grouper(key='FiscalYear', freq='D')]).sum().reset_index())
- Now that dataframes for each quarter or whatever are made:
    - Rows that have the same date need to be combined (or transaction ID? Either? I think date makes more sense)
- Make function to plot graphs and trend lines
- Figure out if its necessary (and how to) scale (parallelization, MapReduce, etc.)

Author: Nathaniel M. Burley
"""
import sys, os, csv
import heapq
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import time
import collections



######################################## PRE-PROCESSING ############################################

# GET OUR COMMAND LINE ARGUEMENTS
datafile_paths = sys.argv[1:len(sys.argv)]
print("Reading in files {}...".format(datafile_paths))


# Function that reads all datafiles into a pandas dataframe (sorted by ascending transaction IDs)
def buildDataFrame(infile_list, columns):
    df = pd.concat([pd.read_csv(f, delimiter='|') for f in infile_list], ignore_index = True)
    df.columns = columns
    df = pd.concat([df.iloc[:,1:3], df.iloc[:,7:8], df.iloc[:,13:20]], axis=1)
    df = df.sort_values('TRANSACTION_ID')
    df['UPC'] = df['UPC'].astype('int64')
    df['Quantity'] = df['Quantity'].astype('int64')
    df['TRANSACTION_ID'] = df['TRANSACTION_ID'].astype('int64')
    df["Date"] = pd.to_datetime(df['Date'])
    return df


# Function that turns the date row into number of days since the earliest date
def convertDate(earliest_date, row_date):
    num_days = row_date.to_pydatetime() - earliest_date.to_pydatetime()
    #print(num_days.days)
    return num_days.days

"""
# Function that aggregates by a given amount of time (week = 'W', month = 'M')
# This one is used for calendar time (week, month, year)
def aggByTime(df, time_period, reset=0):
    if reset:
        agg_dfs = [g.reset_index() for n, g in df.set_index('Date').groupby(pd.Grouper(freq=time_period))]
        return agg_dfs
    else:
        agg_dfs = [g for n, g in df.set_index('Date').groupby(pd.Grouper(freq=time_period))]
        return agg_dfs
"""

# Function that aggregates RAW TRANSACTIONS by fiscal quarter, or month or whatever
#TODO: Raise the number of unique dates required
def aggByTime(transaction_df, time_col):
    # Dataframes that will be returned
    time_dfs = []

    # Gather a list of unique UPCs
    items = transaction_df[time_col].unique()

    # Make a dataframe for each UPC
    for upc in items:
        is_target = transaction_df[time_col] == upc
        og_df = transaction_df[is_target]    #target_upc_df = og_upc_df.groupby('Date')
        num_unique_dates = len(og_df.groupby('Date')['Date'].unique())
        
        # Add UPCs with enough dates to the list of valid ones
        if num_unique_dates >= 5:
            time_dfs.append(og_df)
    
    return time_dfs
    

# Function that makes a dataset for each UPC
# NOTE: Line 98 parameter is SUPER IMPORTANT, are we tracking growth over the quarter per transaction, per date...?
def buildUPCDataFrames(transaction_df):
    # Dataframes that will be returned
    upc_dfs = []

    # Gather a list of unique UPCs
    items = transaction_df['UPC'].unique()

    # Make a dataframe for each UPC
    for upc in items:
        is_target = transaction_df['UPC'] == upc
        og_upc_df = transaction_df[is_target]    #target_upc_df = og_upc_df.groupby('Date')
        num_unique_dates = len(og_upc_df.groupby('Date')['Date'].unique())
        
        # Add UPCs with enough dates to the list of valid ones
        if num_unique_dates > 3:
            upc_dfs.append(og_upc_df)
    
    return upc_dfs


# Function that computes the growth coefficient for a given dataframe
def calculateGrowth(df):
    # Regression X values computed here (days since first date)
    earliest_date = df['Date'].min()
    #df.reset_index(level=0, inplace=True) #Reset the 'Date' column
    df['X'] = df.apply(lambda x: convertDate(earliest_date, x['Date']), axis=1)

    # Regression coefficient computed here (least squares method, link below)
    #(https://stattrek.com/multiple-regression/regression-coefficients.aspx)
    #df.plot(x='X', y='Quantity', style='o')
    #plt.show(block=True)
    x_mean = df['X'].mean()
    y_mean = df['Quantity'].mean()
    df['X_Min_Mean'] = df['X'] - x_mean
    df['X_Min_Mean_Sqrd'] = (df['X'] - x_mean) ** 2
    df['Y_Min_Mean'] = df['Quantity'] - y_mean
    df['(Xi-X)(Yi-Y)'] = df['X_Min_Mean'] * df['Y_Min_Mean']
    try:
        reg_coef = sum(df['(Xi-X)(Yi-Y)']) / sum(df['X_Min_Mean_Sqrd'])
        #print("Growth rate for {}: {}".format(upc, reg_coef))
        return reg_coef
    except:
        print("Error calculating regression coefficient\n\n")
        return 'N/A'


# Function that computes growth per week
# TODO: If there is only like one row per week, just skip
def growthPerTime(upc, upc_df, time_period):
    upc_df_split = aggByTime(upc_df, time_period)
    counter = 1
    growths = {}
    for df in upc_df_split:
        df = df.groupby(['UPC', pd.Grouper(key='Date', freq='D')]).sum().reset_index() # Sum up quantity over dates we want
        df = df[['UPC', 'Date', 'Quantity']]
        #print(df.head(n=10))
        if len(df) < 3:
            print("Not enough rows to calculate.", end=" ")
            growths[counter] = "N/A"
            counter += 1
        else:
            # Growth calculated
            growth = calculateGrowth(df)
            #print("Growth for UPC {} during {} {}: {}".format(upc, counter, time_period, growth), end=" ")
            counter += 1
            growths[counter] = growth
    return growths


# Function that makes sure each growth array has 24 entries
def fillMissingKeys(growth_dict_list):
    allkeys = frozenset().union(*growth_dict_list)
    for i in growth_dict_list:
        for j in allkeys:
            if j not in i:
                i[j]="N/A"
    print(growth_dict_list)
    return growth_dict_list, allkeys
    

# Function that writes growths to a file
def writeToFile(upc, growth_list, outfile_path, columns):
    with open(outfile_path, 'a') as outfile:
        wr = csv.DictWriter(outfile, fieldnames=columns)
        wr.writeheader()
        for row in growth_list:
            wr.writerow(row) 
    outfile.close()



################################### CALCULATE FASTEST GROWING PRODUCTS #############################
if __name__ == "__main__":
    # Data read into data frame; split up by UPC
    columns = ["StoreID", "TRANSACTION_ID", "Date", "TRANS_HOUR", "TRANS_MINUTE", "CASHIER_NUMBER", \
        "TERMINAL_NUMBER", "UPC", "ProductName", "CATEGORY" , "CATEGORY_SUB", "DEPT_KEY_Name", \
        "DEPT_MASTER_Name", "Quantity", "Price", "Sales", "DAY_KEY", "FiscalMonth", "FiscalQtr", "FiscalYear"]
    loaded_df = buildDataFrame(datafile_paths, columns)
    transaction_df_list = buildUPCDataFrames(loaded_df)
    year_growth_list = []
    for upc_df in transaction_df_list:
        upc = upc_df['UPC'].iloc[0]
        print("Calculating growth for UPC {}...".format(upc), end="")
        year_growths = growthPerTime(upc, upc_df, 'FiscalYear')
        year_growths["UPC"] = upc
        year_growth_list.append(year_growths)
        print("...Done!")
        
    
"""
year_df_list = aggByTime(transaction_df_list[0], 'FiscalYear')
print(year_df_list[0].groupby('Date').head(n=82))
practice_year_transactions = year_df_list[0].groupby(['UPC', pd.Grouper(key='Date', freq='D')]).sum().reset_index()
#practice_year_transactions = year_df_list[0].groupby(['Date']).count().add_suffix('_Count').reset_index()
print(practice_year_transactions.head(n=20))
    week_growth_list = []
    month_growth_list = []
    year_growth_list = []
    for upc_df in transaction_df_list:
        # Get the current UPC
        upc = upc_df['UPC'].iloc[0]
        print("Computing growth for UPC {}...".format(upc), end ="")

        # Aggregate by week; compute growth values 
        # TODO: THIS CAN BE SCALED BY PARALLELIZING HORIZONTALLY
        week_growths = growthPerTime(upc, upc_df, 'W')
        week_growths["UPC"] = upc
        week_growth_list.append(week_growths)

        # Aggregate by month; compute growth values 
        # TODO: THIS CAN BE SCALED BY PARALLELIZING HORIZONTALLY
        month_growths = growthPerTime(upc, upc_df, 'M')
        month_growths["UPC"] = upc
        month_growth_list.append(month_growths)

        # Aggregate by year; compute growth values 
        # TODO: THIS CAN BE SCALED BY PARALLELIZING HORIZONTALLY
        year_growths = growthPerTime(upc, upc_df, 'Y')
        year_growths["UPC"] = upc
        year_growth_list.append(year_growths)
        print("Done!")
    
    # Write the computed growths to an outfile (month, in this case)
    filled_week_dict, columns = fillMissingKeys(week_growth_list)
    writeToFile(upc, filled_week_dict, 'Results/weekly_growths.csv', columns)

    # Write the computed growths to an outfile (month, in this case)
    filled_month_dict, columns = fillMissingKeys(month_growth_list)
    writeToFile(upc, filled_month_dict, 'Results/monthly_growths.csv', columns)

    # Write the computed growths to an outfile (month, in this case)
    filled_year_dict, columns = fillMissingKeys(year_growth_list)
    writeToFile(upc, filled_year_dict, 'Results/yearly_growths.csv', columns)
"""
        
    


        



"""
StoreID|TRANSACTION_ID|Date|TRANS_HOUR|TRANS_MINUTE|CASHIER_NUMBER|TERMINAL_NUMBER|UPC|Name|CATEGORY|CATEGORY_SUB_KEY_Name|DEPT_KEY_Name|DEPT_MASTER_KEY_Name|Quantity|Price|Sales|DAY_KEY
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
