# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 09:41:46 2021

@author: matth
"""

from pyspark import SparkConf, SparkContext # boilerplate

conf = SparkConf().setMaster("local").setAppName("VideoGameSales")
sc = SparkContext(conf = conf)

def parseLine(line): # Function to parse data into key-value pairs
    fields = line.split(',')
    title = (fields[0])
    genre = (fields[3])
    publisher = (fields[4])
    global_sales = float(fields[9])
    return (title, (genre, publisher, global_sales))

lines = sc.textFile("file:///sparkcourse/vgsales.csv") # loading data, first row does not contain headers
rdd = lines.map(parseLine)

shooter_filtered = rdd.filter(lambda x: 'Shooter' in x[1][0] ) # games belonging to the Shooter genre

reduce_platform = shooter_filtered.mapValues(lambda x: (x)).reduceByKey(lambda x, y: (x[0], x[1], x[2] + y[2]))
# platform releases treated as separate entry in CSV; combine sales across platforms

flipped = reduce_platform.map(lambda x: (float("{:.2f}".format(x[1][2])), x[0], x[1][1])) # Flips to sort, removes genre from output
# output looks nicer with float("{:.2f}".format())

sales_sorted = flipped.sortByKey(False) # sort by Key (global_sales) in descending order

results = sales_sorted.collect()
print("Video Games in the Shooter Genre by Global Sales (Millions of Copies Sold): ")
for result in results:
    print(result)
    
# Python Console Execution Statement:    
# !spark-submit pysparkVideoGameSales.py

# To Output to Text File:    
# !spark-submit pysparkVideoGameSales.py > vidya-spark.txt