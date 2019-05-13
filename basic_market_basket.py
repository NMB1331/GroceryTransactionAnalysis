"""
This file takes in a raw, pipe delimited file, and writes and save a csv file that contains a simple
list of items, one transaction per line

Use: python make_list_transaction.py /path/to/raw/data/file1 /path/to/datafile2 /...../

NOTES:
- combine date, time, terminal #, storenumber (in file name) into a string to make a unique "transaction id key"

TODO:
- scale this shit....somehow. Figure out how to right Mapper/Reducer for FPGrowth
- write

Author: Nathaniel M. Burley
"""
import sys, os, csv
import heapq
import pandas as pd
import matplotlib.pyplot as plt

target_upc = 4011

######################################## PRE-PROCESSING ############################################

# GET OUR COMMAND LINE ARGUEMENTS
datafile_paths = sys.argv[1:len(sys.argv)]
print("Reading in files {}...".format(datafile_paths))


# HELPER FUNCTIONS
# Gets the index of column names
def getIndex(mystring, columns):
    if (len(columns) > 0):
        return columns.index(mystring)
    else:
        return -1


# Gets entry from current current_row given string current_row name
def getEntry(mystring, current_row, columns):
    if (len(columns) > 0):
        return current_row[columns.index(mystring)]
    else:
        return -1


# Function that builds a transaction key
def buildTransactionKey(current_row, columns):
    transaction_key = getEntry("DATE", current_row, columns) + getEntry("TIME", current_row, columns) \
    + getEntry("TERMINAL_NUMBER", current_row, columns)
    return transaction_key


# Function that reads all datafiles into a pandas dataframe (sorted by ascending transaction IDs)
def buildDataFrame(infile_list):
    df = pd.concat([pd.read_csv(f) for f in infile_list], ignore_index = True)
    df = pd.concat([df.iloc[:,1:3], df.iloc[:,7:8]], axis=1)
    df = df.sort_values('TRANSACTION_ID')
    df['UPC'] = df['UPC'].astype('int64')
    df['TRANSACTION_ID'] = df['TRANSACTION_ID'].astype('int64')
    df["Date"] = df["Date"].astype("datetime64")
    return df


######################################## READ IN DATA ##############################################

# Data read into data file
transaction_df = buildDataFrame(datafile_paths)
#basket_df = transaction_df.merge(transaction_df.groupby('TRANSACTION_ID')['UPC'].apply(list).reset_index())
#print(transaction_df.head(n=20))



##################################### PLOTS HISTOGRAM OF PURCHASES #################################

# Make a data frame with just entries from the target UPC
is_target = transaction_df['UPC'] == target_upc
target_upc_df = transaction_df[is_target].groupby('Date')
print(target_upc_df.head(n=50))
target_upc_df['Date'].count().plot(kind='bar')
plt.show(block=True)



"""
##################################### BUILD LISTS OF TRANSACTIONS ##################################
transaction_list = []
current_basket = []
current_transactionID = 0
counter = 0
for index, row in transaction_df.iterrows():
    if counter == 0:
        current_transactionID = row['TRANSACTION_ID']
        current_basket.append(row['UPC'])
    else:
        if current_transactionID == row['TRANSACTION_ID']:
            current_basket.append(row['UPC'])
        else:
            current_transactionID = row['TRANSACTION_ID']
            transaction_list.append(current_basket)
            current_basket = [row['UPC']]
    counter += 1
    

######################################### ASSOCIATION RULE MINING #########################
# FP-Growth implemented here
patterns = pyfpgrowth.find_frequent_patterns(transaction_list, 2)
rules = pyfpgrowth.generate_association_rules(patterns, 0.9)

# Clean up by removing keys of length 1 (which shouldn't be a thing...)
for tup in list(rules):
    if len(tup) < 2:
        rules.pop(tup, None)

for tup in rules:
    print("Items: {}".format(tup))
"""