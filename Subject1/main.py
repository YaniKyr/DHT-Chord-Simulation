from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pandas as pd
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
    
  
def humTempIndex(df):
    results =[]
    for  row in df.collect():
        results.append(row['temp']-0.55*(1-0.01*row['hum'])*(row['temp']-14.5))

    print(f'max:{max(results)}, min:{min(results)}')

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
tdates = tdf.filter(func.col('value').between(18,22)).groupBy('date').agg(func.count('value'))
print('+----------+')
print("|Rows Found|")
print("+----------+")
print(f'|{tdates.count()}        |')
print("+----------+")
#Top 10 hottest days
tdf.groupBy("date").agg(func.max("value").alias('max_val')).sort(func.desc("max_val")).limit(10).show()

#Top 10 coldest days
tdf.groupBy("date").agg(func.min("value").alias('min_val')).sort(func.asc("min_val")).limit(10).show()

#STD per month 
#TODO GET THE MAX STD
hdf.groupBy(func.month('datetime')).agg(func.stddev('value')).show()


tdf = tdf.withColumnRenamed('value','temp')
hdf = hdf.withColumnRenamed('value','hum')

'''
#Reapeat the calculation of std for specified month

#Get the values column of a specified month
test = hdf.select('datetime','value').where(func.month('datetime')==3)

#Calculate the values of the column
test.select(func.stddev('value')).show()
'''

#Change the name of the columns
#--Join them to a single dataframe
tdf = tdf.withColumnRenamed('value','temp')
hdf = hdf.withColumnRenamed('value','hum')

aux = tdf.join(hdf,tdf['datetime']==hdf['datetime'])

humTempIndex(aux)