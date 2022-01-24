from pyspark import SparkContext
from sys import stdin
#common lines
sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("ERROR")
input = sc.textFile("G:/Trendytech/Spark_dataset/search_data.txt")

#code
words = input.flatMap(lambda x: x.split(" "))

wordcount = words.map(lambda x: (x.lower(), 1))

finalcount = wordcount.reduceByKey(lambda x, y: x + y)

result = finalcount.sortBy(lambda x: x[1], False).collect()

for a in result:
    print(a)

#hold program to check DAG
stdin.readline()