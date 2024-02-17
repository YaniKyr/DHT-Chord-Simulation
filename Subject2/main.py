from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pandas as pd


def getMeanByMonth(df):
    
    df.groupBy(func.month('Date')).\
    agg(func.mean('Open')).\
    sort(func.asc(func.month('Date'))).\
    show(100)

    df.groupBy(func.month('Date')).\
    agg(func.mean('Close')).\
    sort(func.asc(func.month('Date'))).\
    show(100)

    df.groupBy(func.month('Date')).\
    agg(func.mean('Volume')).\
    sort(func.asc(func.month('Date'))).\
    show(100)
    """ 
    Repeat the calclulation for specified month
    temp = df.filter((func.year(df['Date']) == '2005') & (func.month(df['Date'])=='1'))
    
    temp.select(func.mean('Open')).show() 
    """

def higherThan(df):
    df.filter( df['Open'] > 35).agg(func.count('Date')).show()



def highestOpPrice(df,OV):

    maxval = df.select(func.max(OV).alias('maxval')).collect()[0]['maxval']  
    df.select('Date',OV).where(func.col(OV)== maxval).show()


def OpClRangePrice(df):
    #df.groupBy("Date").agg(func.max("Open").alias('max_val')).sort(func.desc("max_val")).limit(10).show()

    maxval = df.select(func.max('Open').alias('maxval')).collect()[0]['maxval']  

    df.select(func.year('Date'),'Open').where(func.col('Open')== maxval).show()


    minval = df.select(func.min('Close').alias('minval')).collect()[0]['minval']  

    df.select(func.year('Date'),'Close').where(func.col('Close')== minval).show()

    


andf = pd.read_csv("./agn.us.txt")
aidf = pd.read_csv("./ainv.us.txt")
aldf = pd.read_csv("./ale.us.txt")

spark = SparkSession.builder.appName('W2').getOrCreate()


agndf = spark.createDataFrame(andf)
ainvdf = spark.createDataFrame(aidf)
alendf = spark.createDataFrame(aldf)

'''
getMeanByMonth(agndf)
getMeanByMonth(ainvdf)
getMeanByMonth(alendf) 

higherThan(agndf)
higherThan(ainvdf)
higherThan(alendf)



highestOpPrice(agndf,'Volume')
highestOpPrice(ainvdf,'Volume')
highestOpPrice(alendf,'Volume')

highestOpPrice(agndf,'Open')
highestOpPrice(ainvdf,'Open')
highestOpPrice(alendf,'Open')

'''

OpClRangePrice(agndf)
OpClRangePrice(ainvdf)
OpClRangePrice(alendf)