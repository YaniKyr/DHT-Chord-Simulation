from pyspark.sql import SparkSession
import pandas as pd
import json
#Temporary parsing in order to prcced
#Problem in data decoding. Functions of panda
#cannot parse this data format. Not exactly json
#Also in this case must deal with lossy data 
def datafileTransform(file):
    testfile = []
    with open(file,'r') as file:
        
        for line in file:
            testfile.append(line.replace('\n',','))
        

    with open('out.txt','w') as file:
        file.write('['+''.join(testfile[:-1])+''.join(testfile[-1][:-1])+']')
    
    
def parse(file):
    
    datafileTransform(file)
    with open('out.txt','r') as fp:
        data_list = json.load(fp)

    dfs = []
    for data in data_list:
        
        df = pd.DataFrame(list(data.items()), columns=['datetime', 'value'])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['value'] = pd.to_numeric(df['value'])
        dfs.append(df)
    pdd = pd.concat(dfs,ignore_index=True)
    print(pdd)


parse('./hum.txt')
