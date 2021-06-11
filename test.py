from pyspark import SparkConf, SparkContext
from itertools import islice
import collections


MY_APP_NAME = "MyFirstSparkApp"
URL_DATA = "data/ratings.csv"
#Context Setup
config =SparkConf().setMaster("local").setAppName(MY_APP_NAME)
sc = SparkContext(conf=config)

rdd = sc.textFile(URL_DATA)
# Removing CSV header
rdd = rdd.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

ratings = rdd.map(lambda x: x.split(",")[2])
results = ratings.countByValue()

sorted_results = collections.OrderedDict(sorted(results.items()))
for k, val in sorted_results.items():
    print("key: {} \t\t value:{} ".format(k,val))



