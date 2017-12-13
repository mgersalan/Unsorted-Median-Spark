% pyspark  # forces zeppelin to use pyspark interpreter
# Runs either on local zepllin or AWS

from pyspark import SparkContext
import sys
import os
import random
import math


def cache_RDD(fn):
  data = sc.textFile(fn)
  dataRDD = data.map(lambda s: float(s))
  dataRDD.cache()
  return dataRDD


def calculate_avg(dataRDD):
  # data = sc.textFile(fn)
  # dataRDD = data.map(lambda s: float(s))
  myAvg = dataRDD.sum() / dataRDD.count()
  return myAvg


def calc_minimum(dataRDD):
  # data = sc.textFile(fn)
  # dataRDD = data.map(lambda s: float(s))
  min_value = dataRDD.min()
  return min_value


def maximum(dataRDD):
  # data = sc.textFile(fn)
  # dataRDD = data.map(lambda s: float(s))
  max_value = dataRDD.max()
  return max_value


def calc_val(dataRDD):
  # data = sc.textFile(fn)
  # dataRDD = data.map(lambda s: float(s))
  variance = dataRDD.variance()
  return variance


def find_median(dataRDD):
  n = dataRDD.count()
  k = round(n / 2) + 1

  if (n % 2 != 0):
    rez = find_kth_element(dataRDD, k)
    return rez
  else:
    rez1 = find_kth_element(dataRDD, k)
    rez2 = find_kth_element(dataRDD, k - 1)
    print("median1 = " + str(rez1))
    print("median2 = " + str(rez2))
    rez = (rez1 + rez2) / 2
    return rez


def find_kth_element(Set, k):
  n = Set.count()
  pivot = Set.takeSample(False, 1, seed=0)
  pivot_num = pivot[0]

  rdd_smaller = Set.filter(lambda x: x < pivot_num)
  rdd_greater = Set.filter(lambda x: x > pivot_num)

  if k == (rdd_smaller.count() + 1):
    # print ("3")
    # val = Set.zipWithIndex().filter(lambda (key,index) : index == k-1).map(lambda (key,index): key).collect()[0]
    return pivot_num

  if k <= (rdd_smaller.count()):
    # print ("1")
    val = find_kth_element(rdd_smaller, k)
    return val

  if k > (rdd_smaller.count() + 1):
    # print ("2")
    val = find_kth_element(rdd_greater, k - (rdd_smaller.count() + 1))
    return val


cachec_RDD = cache_RDD('/Users/Gur/Desktop/data-1.txt')

myAvg = calculate_avg(cachec_RDD)
result = "The average of data set is " + str(myAvg)
print(result)

myMin = calc_minimum(cachec_RDD)
result1 = "The minimum of data set is " + str(myMin)
print(result1)

myMax = maximum(cachec_RDD)
result2 = "The maximum of data set is " + str(myMax)
print(result2)

myVar = calc_val(cachec_RDD)
result3 = "The variance of data set is " + str(myVar)
print(result3)

### median
median = find_median(cachec_RDD)
print("Median is equal to " + str(median))