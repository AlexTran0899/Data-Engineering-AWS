import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

path = "/Users/at/Documents/udacity/learning/Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/data/sparkify_log_small.json"

user_log_df = spark.read.json(path)

outpath = 'sparkify_log_small_csv'

user_log_df.write.mode('overwrite').save(outpath, format='csv', header=True)

user_log_2_df = spark.read.csv(outpath, header=True)
user_log_2_df.select('userID').show()

