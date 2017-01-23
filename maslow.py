import pandas as pd
import json
import numpy as np
from datetime import datetime
import sys
import re
import os

""" Data read-in. This part of the code checks the file extension of the user-provided 
file from the command line"""

file_name = sys.argv[1]
extension = re.sub(".*[\.]","",file_name)
out_name = re.sub("[\.].*","",file_name)

if extension == 'csv':
    df = pd.read_csv(file_name)
elif extension == 'json':
    df = pd.read_json(file_name)
else:
    print 'Please enter a valid file type.'

print "We've got data..."

"""Lower-casing columns in dataframe"""
lower_cols = []
for x in df.columns:
    x = x.lower()
    x = re.sub("\s","_",x)
    lower_cols.append(x)

df.columns = lower_cols

"""Now we're going to offer the user a menu of columns"""

print "Please review the following column headers and be prepared to answer a few questions about them:"
for x in df.columns:
    print x

try:
    date_field_identify = raw_input("If you have a date column, please identify it now. If not, please press ENTER.\n")
    if date_field_identify:
        df['new_date'] = df[date_field_identify].astype(datetime).astype(str)
    if not date_field_identify:
        raise ValueError('empty string')
except ValueError as e:
    print(e)

print "Great. Now, please select some metadata fields from the column heads above. Please enter your metadata \n as a list, like so, \'a,b,c\'. (NO SPACES PLEASE!)\n"
meta1 = raw_input("ENTER METADATA LIST HERE\n").lower()
meta1 = meta1.split(",")
print meta1

print "OK. Please select the fields that you wish to aggregate from the column heads above. Please also enter these as a list. \n"
num1 = raw_input("ENTER FIELDS TO BE CALCULATED LIST HERE\n").lower()
num1 = num1.split(",")
print num1

print "Finally, please select the fields you wish to group. Enter these fields in order of hierarchical descendancy. \n"
hier1 = raw_input("ENTER HIERARCHY FIELDS HERE\n").lower()
hier1 = hier1.split(",")
print hier1

#Reassigning for housekeeping purposes.
meta = meta1
num_fields = num1
group1 = hier1

#Make list of group lists
def group_drill(start_keys):
    all_groups = []
    while len(start_keys) > 0:
        all_groups.append(start_keys)
        start_keys = start_keys[:-1]
    return all_groups

all_my_groups = group_drill(group1)
print all_my_groups

def group_all(gl, frame):
    #print gl
    grouped = []
    names = []
    print names
    frames = []
    for x in gl:
        merge_name = str(x[-1])
        name = frame.groupby(x)
        print "These are your group keys: %s" % x
        calcs = name[num_fields].agg([np.sum, np.mean, np.std, np.count_nonzero]).fillna('null')
        calcs.columns = ['%s%s' % (a, '_%s' % b if b else '') for a, b in calcs.columns]
        calcs = calcs.reset_index()
        firsts = name[meta].first().reset_index(x)
        merged = pd.merge(calcs,firsts).to_dict(orient='records')
        grouped.append(merged)
    return grouped
    
everything = group_all(all_my_groups,df)

def make_json(all_jsons,my_keys):
    print my_keys[-1]
    counter = 0
    while len(my_keys) > 1:
        print "ALL JSONS EQUAL %s" % str(len(all_jsons[0]))
        counter += 1
        for each in all_jsons[1]:
            greek = []
            for other in all_jsons[0]:
                if all(each[key] == other[key] for key in my_keys[:-1]):
                    greek.append(other)
            each.update({my_keys[-1]+"_list":greek})
        del all_jsons[0]
        my_keys = my_keys[:-1]
            
        print "%sx through" % counter
    return all_jsons[0]

if not os.path.exists(hier1[0]+"_"+out_name):
    os.makedirs(hier1[0]+"_"+out_name)
            
for i in make_json(everything,group1):
    with open(hier1[0]+"_"+out_name + '/' + i[hier1[0]]+'.json', 'w') as fh:
            json.dump(i, fh)




  


