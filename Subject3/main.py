from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType
import pyspark.sql.functions as F
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression




def importData():
    df =pd.read_excel('./tour_occ_ninat.xlsx')
    

    # Isoalation of useful data
    df = df.rename(columns=df.loc[7])
    df = df.loc[8:]
    df= df.replace(':',None)
    df_original = df.transpose()
    
    #Reorganize dataframe after transpose
    df_original.reset_index(inplace=True)
    df_original = df_original.rename(columns=df_original.loc[0])
    df_original = df_original.loc[1:]
    #print(df_original.isnull().sum().sort_values(ascending=False))

    df_original['Switzerland'] = df_original['Switzerland'].fillna(0)
    df_original['Turkey'] = df_original['Turkey'].fillna(df_original['Turkey'].mean())
    df_original['Montenegro'] = df_original['Montenegro'].fillna(df_original['Montenegro'].mean())
    df_original['Serbia'] = df_original['Serbia'].fillna(df_original['Serbia'].mean())
    df_original['Ireland'] = df_original['Ireland'].fillna(df_original['Ireland'].mean())
    df_original['United Kingdom'] = df_original['United Kingdom'].\
        fillna(df_original['United Kingdom'].mean())
    df_original['Former Yugoslav Republic of Macedonia, the'] =\
          df_original['Former Yugoslav Republic of Macedonia, the'].\
            fillna(df_original['Former Yugoslav Republic of Macedonia, the'].mean())
    #None value handling
    """ for index,row in df_original.iterrows():
        df_original.loc[index] = df_original.loc[index].fillna(df_original.loc[index].mean()) """
    return df_original

df = importData()


spark = SparkSession.builder.appName('W1').getOrCreate()

sdf = spark.createDataFrame(df)

countries =["Spain","Bulgaria","France","Hungary","Germany"]

def HigherCountryMargin():
    # Filter the DataFrame to include only the years from 2007 to 2014
    filtered_df = sdf.filter((F.col("GEO/TIME") >= 2007) & (F.col("GEO/TIME") <= 2014))

    # Calculate the mean for each country
    mean_df = filtered_df.select(
        [F.mean(col_name).alias(col_name) for col_name in sdf.columns[1:]])
    mean_df.show()


def higherThan( countries, target ="Greece"):
    
    for country in countries:
        ndf= 0
        ndf = sdf.select(F.col(target), F.col(country))
        df_new = ndf.withColumn('res', ndf[target] > ndf[country]).groupby('res').count()
        print('Compare',target,'with',country)
        df_new.select('*',F.when(df_new.res == True,1).otherwise(0)).show()

#1
#max_values_df = sdf.select("Name", F.greatest(*[F.col(c) for c in df.columns[1:]]).alias("Max_Value"))

countries = [x.name for x in sdf.schema.fields]
countries = countries[1:]
'''
tt = sdf.withColumn('new',(F.greatest(*countries)))

for country in countries:
    ll = tt.select('GEO/TIME',country).where(tt[country] == tt.new)
    if len(ll.head(1))!=0:
        ll.show()

'''


ll=sdf
for country in countries:
    
    for cj in countries:
        if country == cj: continue
        #if 'Belgium' == cj: continue
        ll= ll.withColumn(cj,(ll[country] < ll[cj]).cast("int").alias('compare'))
        
        
        #Make trues to ones
    ll = ll.drop(country)
    print("======================================")
    print(f'==========={country}=================')
    print("======================================")
    c97 = [x.name for x in ll.schema.fields]
    c97 = c97[1:]
    ff = ll.select('GEO/TIME',(sum(F.col(col_name) for col_name in c97)).alias('sum'))
    
    max_value = ff.agg({'sum':'max'}).collect()[0][0]
    ff.filter(ff['sum'] == max_value).show()
    
    ll =sdf




#Add two columns

#Get the max from each row
#print yesars
