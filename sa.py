from __future__ import print_function, division
import json, os, datetime, collections, commands
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

def main():
  if not os.path.exists("./click_data_sample.csv"):
    print("csv file not found at master node, will download and copy to HDFS")
    commands.getoutput("wget -q http://image.gihyo.co.jp/assets/files/book/2015/978-4-7741-7631-4/download/click_data_sample.csv")
    commands.getoutput("hadoop fs -copyFromLocal -f ./click_data_sample.csv /mnt/sdb1/hadoop/")

  raw_log = sc.textFile("/mnt/sdb1/hadoop/201702_huy_1.5m.csv")
  header = raw_log.first()
  log = raw_log \
              .filter(lambda x:x !=header)\
	      .map(lambda line: line.split(","))\
              .map(lambda line: [line[0]])

  print(log.take(3))

if __name__ == '__main__':
  conf = (SparkConf().setMaster("local").setAppName("benchmark").set("spark.executor.memory", "32g"))
  sc = SparkContext(conf=conf)
  main()
