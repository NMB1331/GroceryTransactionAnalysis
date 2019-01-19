"""
This file takes in a raw, pipe delimited file, and writes and save a csv file that contains a simple
list of items, one transaction per line

Use: python make_list_transaction.py /path/to/raw/data/file1 /path/to/datafile2 /...../

NOTES:
- combine date, time, terminal #, storenumber (in file name) into a string to make a unique "transaction id key"
- down the road, use UPC code instead of item number (more specific)

Author: Nathaniel M. Burley
"""
import sys, os, csv
import pyfpgrowth
import heapq

######################################## PRE-PROCESSING #################################

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


# ITERATE THROUGH FILE, ADDING EVERY TRANSACTION TO AN ARRAY
basket_items = []
transactions_list = []
for f in datafile_paths:
    with open(f, 'r') as infile:
        data_csvfile = csv.reader(infile, delimiter='|')
        rows = [row for row in data_csvfile]
        columns = rows[0]
        current_transaction_id = getEntry("TRANSACTION_ID", rows[1], columns)
        for i in range(1, len(rows)):
            row = rows[i]
            if getEntry("TRANSACTION_ID", row, columns) == current_transaction_id:
                basket_items.append(getEntry("ITEM", row, columns))
            else:
                transactions_list.append(basket_items)
                basket_items = []
                basket_items.append(str(getEntry("ITEM", row, columns)))
                current_transaction_id = getEntry("TRANSACTION_ID", row, columns)

# PRINTS A SAMPLE OF THE TRANSACTIONS FOR DEBUGGING
#for i in range(50, 100):
 #   print(transactions_list[i])



######################################### ASSOCIATION RULE MINING #########################
# FP-Growth implemented here
patterns = pyfpgrowth.find_frequent_patterns(transactions_list, 2)
rules = pyfpgrowth.generate_association_rules(patterns, 0.9)

# Clean up by removing keys of length 1 (which shouldn't be a thing...)
for tup in list(rules):
    if len(tup) < 2:
        rules.pop(tup, None)

for tup in rules:
    print("Items: {}".format(tup))
