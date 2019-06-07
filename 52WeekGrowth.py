"""
This file takes a comma delimited csv file of transactions as an input, and computes the 52 week 
growths for each UPC, and outputs a .csv file with the results of the calculations

Use: python compute_growth.py /path/to/transaction/file.csv

TODO:
- Remove all UPCs where their total number of transactions is below the median (possibly mean)
- Figure out how to sort by GROWTHS for writing to the file. Not keys.
- Try to implement parallelization, so each core is being used. The flow should be:
    - Preprocess; make list of UPC dataframes
    - Split that list into n other lists (n = number of cores)
    - Then on each core, compute growths, and push the resulting UPC:Growth pair to list
    - At the end, sort and write to outfile like normal
    See this link for tips: https://stackoverflow.com/questions/45887641/running-python-on-multiple-cores

Author: Nathaniel M. Burley
"""
import pandas as pd
import matplotlib.pyplot as plt
from collections import OrderedDict
import sys, os, csv, datetime, time, collections



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
    df = removeDates(df)
    return df


# Function that removes dates (in this case, dates older than 52 weeks from the most recent one)
def removeDates(df):
    most_recent_date = df['Date'].max()
    one_year_ago = most_recent_date - datetime.timedelta(weeks=52)
    print("Original number of records: {}".format(len(df)))
    df = df[df['Date'] >= one_year_ago]
    print("Number of transactions within last 52 weeks: {}".format(len(df)))
    return df


# Function that turns the date row into number of days since the earliest date
def convertDate(earliest_date, row_date):
    num_days = row_date.to_pydatetime() - earliest_date.to_pydatetime()
    #print(num_days.days)
    return num_days.days
    

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


# Function that computes the median number of transactions (or maybe mean down the road)
#TODO: IMPLEMENT; TEST
def medianNumTransactions(df):
    pass


# Function that drops UPC data frames from list if they have less than the median number of transactions
#TODO: IMPLEMENT; TEST
def dropUPCsBelowMedian(upc_df_list):
    pass


# Function that splits remaining UPC dataframes after pre-processing into n lists (n = num. CPU cores)
#TODO: IMPLEMENT; TEST
def buildParallelLists(upc_df_list):
    pass


# Function that computes growths for one of the lists; to be run in parallel
#TODO: IMPLEMENT; TEST
def computeGrowthsOnCore(upc_df_list):
    pass


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
    #upc_df_split = aggByTime(upc_df, time_period)
    upc_df_split = [upc_df]
    counter = 1
    for df in upc_df_split:
        df = df.groupby(['UPC', pd.Grouper(key='Date', freq='D')]).sum().reset_index() # Sum up quantity over dates we want
        df = df[['UPC', 'Date', 'Quantity']]
        #print(df.head(n=10))
        if len(df) < 3:
            print("Not enough rows to calculate.", end=" ")
            return "N/A"
        else:
            # Growth calculated
            growth = calculateGrowth(df)
            print("52 week growth for UPC {}: {}".format(upc, growth))
            return growth


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
# TODO: Figure out why it isn't actually opening and writing to the file...?
def writeToFile(growth_list, outfile_path, columns):
    with open(outfile_path, 'w+') as outfile:
        print(("%s,%s\n"%columns))
        outfile.write("%s,%s\n"%columns)
        for k in growth_list.keys():
            outfile.write("%d,%f\n"%(k,growth_list[k]))
    outfile.close()



################################### CALCULATE FASTEST GROWING PRODUCTS #############################
if __name__ == "__main__":
    # Data read into data frame; split up by UPC
    #TODO: Split into num_cores lists; run growth calculations in parallel
    columns = ["StoreID", "TRANSACTION_ID", "Date", "TRANS_HOUR", "TRANS_MINUTE", "CASHIER_NUMBER", \
        "TERMINAL_NUMBER", "UPC", "ProductName", "CATEGORY" , "CATEGORY_SUB", "DEPT_KEY_Name", \
        "DEPT_MASTER_Name", "Quantity", "Price", "Sales", "DAY_KEY", "FiscalMonth", "FiscalQtr", "FiscalYear"]
    loaded_df = buildDataFrame(datafile_paths, columns)
    transaction_df_list = buildUPCDataFrames(loaded_df)
    year_growth_dict = {}

    # Growths computed for the most recent 52 weeks of data
    #TODO: Push this into function; make it run in parallel on every CPU core
    for upc_df in transaction_df_list:
        upc = upc_df['UPC'].iloc[0]
        print("Calculating growth for UPC {}...".format(upc), end="")
        year_growth = growthPerTime(upc, upc_df, 'Week')
        year_growth_dict[upc] = year_growth

    # Growths sorted; written to an outfile
    sorted_growths = dict(OrderedDict(sorted(year_growth_dict.items()))) # Possibly use something else to improve speed
    writeToFile(sorted_growths, "52WeekGrowths.csv", ("UPC", "52WeekGrowth"))
