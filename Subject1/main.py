from pyspark.sql import SparkSession
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
    
  


df = parse('./tempm.txt')
df = handleMissingData(df)
#Check for missing Values



#Fill the missing values

#----------------------
#Start SparkSession
spark = SparkSession.builder.appName('W1').getOrCreate()

pdf = spark.createDataFrame(df)

print(pdf.select('datetime','value').where('MIN(value)==18 AND MAX(value)==22').show(100))