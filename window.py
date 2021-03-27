import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
sc = SparkSession.builder.appName("SimpleApp").getOrCreate()

df = pd.DataFrame()
df['id'] = np.random.randint(1, 1000, size=150000)
df['id'] = df['id'].map(lambda x: 100 if x % 2 == 0 else x)
df['timestamp'] = pd.date_range(start=pd.Timestamp('2020-01-01'), periods=len(df), freq='60s')
sdf = sc.createDataFrame(df)
sdf = sdf.withColumn("amt", F.rand()*100)
w = Window.partitionBy("id").orderBy("timestamp")

sdf = sdf.withColumn("new_col", F.lag("amt").over(w) + F.lead("amt").over(w))
x = sdf.toPandas()


    







        

