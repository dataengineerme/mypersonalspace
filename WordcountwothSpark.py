"""
Below is a wordcount program 
where we are trying to count the words from a file 
file name: inputtext.txt
using simple transformations flatMap, Map, 
reduceByKey
and then finally saving the result to an output textfile
"""
from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
builder. \
config('spark.ui.port','0'). \
config("spark.sql.warehouse.dir", f"/user/itv000173/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

rdd1 = spark.sparkContext.textFile("filepath")
rdd2 = rdd1.flatMap(lambda x : x.split(" "))
rdd3 = rdd2.map(lambda word : (word,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)
#rdd4.collect()

rdd4.saveAsTextFile("Output_file_directorypath")
