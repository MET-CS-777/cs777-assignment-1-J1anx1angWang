from __future__ import print_function

import os
import sys

from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *



#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

# Function to calculate money per minute for each trip
def parse_and_calculate(fields):
    driver_id = fields[1]  # Driver ID (hack license)
    trip_time_in_secs = float(fields[4])  # Trip duration in seconds
    total_amount = float(fields[16])  # Total amount earned on the trip
    money_per_minute = total_amount / (trip_time_in_secs / 60)  # Convert seconds to minutes
    return (driver_id, (money_per_minute, 1))  # Return driver_id and a tuple (money_per_minute, 1)



#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])
    
    #Task 1
    #Your code goes here
    clean_rdd = rdd.map(lambda line: line.split(",")).filter(correctRows)
    
    # Map step: (Taxi ID, Driver ID)
    taxi_driver_pairs = clean_rdd.map(lambda p: (p[0], p[1]))

    # Reduce step: Count distinct drivers per taxi
    taxi_driver_count = taxi_driver_pairs.distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

    # Sort and get top 10
    top10_taxis = taxi_driver_count.sortBy(lambda x: x[1], ascending=False).take(10)

    # Save the results to the output path
    sc.parallelize(top10_taxis).coalesce(1).saveAsTextFile(sys.argv[2])


    # Task 2
    # Your code goes here
    # Calculate money per minute for each driver and trip
    money_per_minute_rdd = clean_rdd.map(parse_and_calculate)

    # Aggregate by driver_id (calculate total money per minute and count of trips for each driver)
    driver_aggregates = money_per_minute_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Calculate the average money per minute for each driver
    driver_avg_money_per_minute = driver_aggregates.mapValues(lambda v: v[0] / v[1])

    # Get the top 10 drivers by average money per minute
    top_10_drivers = driver_avg_money_per_minute.sortBy(lambda x: x[1], ascending=False).take(10)

    # Save the results to the output path
    sc.parallelize(top_10_drivers).coalesce(1).saveAsTextFile(sys.argv[3])

    # Stop the SparkContext
    sc.stop()