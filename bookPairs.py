# set up SparkContext for WordCount application
from pyspark import SparkContext
sc = SparkContext("local", "WordCount")

def process(line):
    user = int(line.split(":")[0])
    books = [int(x) for x in line.split(":")[1].split(",")]
    pairs = []
    l=len(books)
    for i in range(l):
        for j in range(i+1,l):
            pairs.append(((books[i],books[j]), 1))
    return pairs

lines = sc.textFile("/Users/xilaizhang/Desktop/past-classes/databaseExtras/project5/sample.txt", 4)
words = lines.flatMap(lambda line: process(line))

wordCounts = words.reduceByKey(lambda a, b: a+b)
result = wordCounts.filter( lambda y : y[1] > 20)
result.saveAsTextFile("/Users/xilaizhang/Desktop/output")


    







        

