# from pyspark.sql import SparkSession

# logFile = "/Users/xilaizhang/Desktop/input.txt"  # Should be some file on your system
# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# logData = spark.read.text(logFile).cache()

# numAs = logData.filter(logData.value.contains('a')).count()
# numBs = logData.filter(logData.value.contains('b')).count()

# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

# spark.stop()
from math import exp
from random import randint
from datetime import datetime
 
def count_elements(splitIndex, iterator):
    n = sum(1 for _ in iterator)
    yield (splitIndex, n)
 
def get_part_index(splitIndex, iterator):
    for it in iterator:
        yield (splitIndex, it)
 
num_parts = 16
# create the large skewed rdd
from pyspark import SparkContext
sc = SparkContext("local", "article-fixed")
skewed_large_rdd = sc.parallelize(range(0,num_parts), num_parts).flatMap(lambda x: range(0, int(exp(x))))
 
print("skewed_large_rdd has %d partitions."%(skewed_large_rdd.getNumPartitions()))
print ("The distribution of elements across partitions is: %s"
%(str(skewed_large_rdd.mapPartitionsWithIndex(lambda ind, x: count_elements(ind, x)).take(num_parts))))
 
# put it in (key, value) form
skewed_large_rdd = skewed_large_rdd.mapPartitionsWithIndex(lambda ind, x: get_part_index(ind, x)).cache()
skewed_large_rdd.count()

small_rdd = sc.parallelize(range(0,num_parts), num_parts).map(lambda x: (x, x)).cache()
small_rdd.count()

# t0 = datetime.now()
# result = skewed_large_rdd.leftOuterJoin(small_rdd)
# result.count() 
# print("The direct join takes %s"%(str(datetime.now() - t0)))

N = 100 # parameter to control level of data replication
small_rdd_transformed = small_rdd.cartesian(sc.parallelize(range(0, N))).map(lambda x: ((x[0][0], x[1]), x[0][1])).coalesce(num_parts).cache() # replicate the small rdd
small_rdd_transformed.count()
skewed_large_rdd_transformed = skewed_large_rdd.map(lambda x: ((x[0], randint(0, N-1)), x[1])).partitionBy(num_parts).cache() # add a random int to forma  new key
skewed_large_rdd_transformed.count()
 
t0 = datetime.now()
result = skewed_large_rdd_transformed.leftOuterJoin(small_rdd_transformed)
result.count()
print("The hashed join takes %s"%(str(datetime.now() - t0)))


    







        

