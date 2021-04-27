import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sc = SparkSession.builder.appName("cacheApp").getOrCreate()

df = sc.read.csv("/Users/xilaizhang/Desktop/spark-skew/input.txt", inferSchema=True, header=True)
df2 = df.filter(col("State") == "PR").cache()
df2.show(n=2)

print(df2.count())
df2Persist = df2.persist( pyspark.StorageLevel.MEMORY_AND_DISK_2 )

df3 = df2.filter(col("Zipcode") == 704)

print(df3.count())
print(df2.count())




    







        

