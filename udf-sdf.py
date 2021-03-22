# UDF vs Spark function
from faker import Factory
from pyspark.sql.functions import lit, concat, udf
from pyspark.sql import SparkSession
fake = Factory.create()
fake.seed(4321)

# Each entry consists of last_name, first_name, ssn, job, and age (at least 1)
from pyspark.sql import Row
def fake_entry():
  name = fake.name().split()
  return (name[1], name[0], fake.ssn(), fake.job(), abs(2016 - fake.date_time().year) + 1)

# Create a helper function to call a function repeatedly
def repeat(times, func, *args, **kwargs):
    for _ in range(times):
        yield func(*args, **kwargs)
data = list(repeat(5000, fake_entry))
print(len(data))
data[0]

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
dataDF = spark.createDataFrame(data, ('last_name', 'first_name', 'ssn', 'occupation', 'age'))
dataDF.cache()

# concat_s = udf(lambda s: s+ 's')
# udfData = dataDF.select(concat_s(dataDF.first_name).alias('name'))
# udfData.count()

spfData = dataDF.select(concat(dataDF.first_name, lit('s')).alias('name'))
spfData.count()