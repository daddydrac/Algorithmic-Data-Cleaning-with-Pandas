import pandas as pd
from datetime import datetime
import numpy as np 
import pydash as _

###  Read in CSV files
df1 = pd.read_csv('./input/Task_Data_1.csv')
df2 = pd.read_csv('./input/Task_Data_2.csv')

###  Use Pandas DF to merge by common key, "item_id" to ensure 
###  that rows from both CSVs  matched
merged_by_id = df1.merge(df2, how="left", on="item_id")

###  Format values for result Data1.mp1 * data2.mp2
merged_by_id['result'] = merged_by_id.mp1 * merged_by_id.mp2

###  Format values for date as Data2.date in YYYY-MM-DD
merged_by_id['date_new'] = pd.to_datetime(merged_by_id['date']).dt.strftime('%Y-%m-%d')

###  Drop missing values: Drop all rows from col 'value' 
###  inside Pands DF that are NaN or Null
merged_by_id = merged_by_id[pd.notnull(merged_by_id['value'])]

###  Space in mem to hold data
data = [] 

### Python struct
init = {
    'prev': None,
    'curr': None,
    'result': []
}

###  Get data in array so pseudo rolling windows 
### with conditions can operate on data
for x in merged_by_id.loc[:, 'value']:
    data.append(x)

### Algorithmic accumulator that walks arrays right (reduceRight)
### and handles the conditions of Data1.value or 
### (data1[row-1].value + data1[row+1].value) / 2 
### Or 1 if there is no [row-1] or [row+1] values 
### without mutations to variables, no loops, and zero non deterministic 
### code design patterns

def fn(acc, curr):

    if acc['prev'] == None and acc['curr'] == None:
        return {
            'prev': None,
            'curr': curr,
            'result': [1]
        }

    elif acc['curr'] != None and acc['prev'] == None:
        return {
            'prev': acc['curr'],
            'curr': curr,
            'result': acc['result']
        }
    elif acc['curr'] != None and acc['prev'] != None:
        return {
            'prev': acc['curr'],
            'curr': curr,
            'result': [ (acc['prev']+curr) / 2 ] + acc['result']
        }


def det():
    if len(data) == 1:
        return data
    else:
        return [1] + _.reduce_right(data, fn, init)['result']

### print to verify Pandas DF, output desired CSV file in /output folder 
result = det()
df = pd.DataFrame({"item_identifier":merged_by_id['item_id'],"result":merged_by_id['result'],"date_new":merged_by_id['date_new'],"new_value":result})
print(df)
df.to_csv(r'./output/clean_data.csv')