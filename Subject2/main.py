from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pandas as pd


def getMeanByMont(df):
    df.groupBy(func.year('Date'),func.month('Date')).\
    agg(func.mean('Open')).\
    sort(func.asc(func.year('Date')) , func.asc(func.month('Date'))).\
    show()

    df.groupBy(func.year('Date'),func.month('Date')).\
    agg(func.mean('Close')).\
    sort(func.asc(func.year('Date')) , func.asc(func.month('Date'))).\
    show()

    """ 
    Repeat the calclulation for specified month
    temp = df.filter((func.year(df['Date']) == '2005') & (func.month(df['Date'])=='1'))
    
    temp.select(func.mean('Open')).show() 
    """

def higherThan(df):
    df.filter( df['Open'] > 35).agg(func.count('Date')).show()



def highestOpPrice(df,OV):

    # Test: Find the top 10 rows
    #df.groupBy("Date").agg(func.max("Open").alias('max_val')).sort(func.desc("max_val")).limit(10).show()



    #Iteration #1. The query calculates the max of a column and searches for dublicates
    if OV == 'Volume':
        maxval = df.select(func.max(OV).alias('maxval')).collect()[0]['maxval']  
        df.select('Date',OV).where(func.col(OV)== maxval).show()

    #Iteration #2. The querey compares the two requested columns and scans for the same 
    #attributes
    df.select('Date',OV,'High').where(func.col(OV) == func.col('High')).show()
    df.select('Date',OV,'High').where(func.col(OV) == func.col('High')).show()
    


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

""" 
getMeanByMont(agndf)
getMeanByMont(ainvdf)
getMeanByMont(aldf) 

higherThan(agndf)
higherThan(ainvdf)
higherThan(alendf)



highestOpPrice(alendf,'Volume')
highestOpPrice(agndf,'Volume')
highestOpPrice(aidf,'Volume')

highestOpPrice(alendf,'Open')
highestOpPrice(agndf,'Open')
highestOpPrice(ainvdf,'Open')
 """

highestOpPrice(agndf,'Open')

