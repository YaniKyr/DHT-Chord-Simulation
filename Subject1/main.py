from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, max, asc,  min,col, count, month, stddev
import pandas as pd
import pyspark
import json
import os
#Fixes in the dataframe must be done
#Best solution 3 columns, 1. Data, 2. Datetime, 3. Values

#Handle the data. Transform them into a proper json format

def handleMissingData(df):
    msdate = df[df['value'].isnull() == True]['datetime']
    
    for i,_ in enumerate(msdate):
        index = df['datetime'].dt.date==msdate.iloc[i].date()
        df[index] = df[index].fillna(df[index].mean())

    return df


def datafileTransform(file):
    testfile = []
    with open(file,'r') as file:
        
        for line in file:
            testfile.append(line.replace('\n',','))
        

    file =  open('temp.txt','w')
    file.write('['+''.join(testfile[:-1])+''.join(testfile[-1][:-1])+']')
    
    
def parse(file):
    
    datafileTransform(file)

    
    with open('./temp.txt','r') as fp:
        data_list = json.load(fp)
    fp.close()
    os.remove('./temp.txt')

    
    dfs = []
    for data in data_list:
        
        df = pd.DataFrame(list(data.items()), columns=['datetime', 'value'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['value'] = pd.to_numeric(df['value'])
        dfs.append(df)
    pdd = pd.concat(dfs,ignore_index=True)
    pdd.sort_values(by='datetime',inplace=True)
    
    return pdd
    
  


tdf = parse('./tempm.txt')
tdf = handleMissingData(tdf) 

hdf = parse('./hum.txt')
hdf = handleMissingData(hdf)
#Check for missing Values



#Fill the missing values

#----------------------

#Start SparkSession

spark = SparkSession.builder.appName('W1').getOrCreate()

tdf['date']=pd.to_datetime(tdf['datetime']).dt.date
tdf = spark.createDataFrame(tdf)

hdf = spark.createDataFrame(hdf)


#Find the temperature between a specified range
tdates = tdf.filter(col('value').between(18,22)).groupBy('date').agg(count('value'))
print('+----------+')
print("|Rows Found|")
print("+----------+")
print(f'|{tdates.count()}        |')
print("+----------+")
#Top 10 hottest days
tdf.groupBy("date").agg(max("value").alias('max_val')).sort(desc("max_val")).limit(10).show()

#Top 10 coldest days
tdf.groupBy("date").agg(min("value").alias('min_val')).sort(asc("min_val")).limit(10).show()

#STD per month
hdf.groupBy(month('datetime')).agg(stddev('value')).show()


'''
#Reapeat the calculation of std for specified month

#Get the values column of a specified month
test = hdf.select('datetime','value').where(month('datetime')==3)

#Calculate the values of the column
test.select(stddev('value')).show()
'''