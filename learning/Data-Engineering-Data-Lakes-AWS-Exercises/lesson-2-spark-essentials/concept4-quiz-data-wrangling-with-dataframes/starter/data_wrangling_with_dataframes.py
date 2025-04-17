from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, col
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 
path = "/Users/at/Documents/udacity/learning/Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/data/sparkify_log_small.json"
spark = SparkSession.builder.appName("Data Frames practice").getOrCreate()
logs_df = spark.read.json(path)

logs_df.printSchema()
# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?
# TODO: write your code to answer question 1
visited_pages = logs_df.filter(logs_df.userId == '').select('page').dropDuplicates()
all_pages_df = logs_df.select('page').dropDuplicates()

for row in set(all_pages_df.collect()) - set(visited_pages.collect()):
    print(row.page)

# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# ANSWER: most likely a user who have not signed up yet or is signed out currently

# TODO: use this space to explore the behavior of the user with an empty string


# # Question 3
# 
# How many female users do we have in the data set?
# TODO: write your code to answer question 3
female_user_count = logs_df.filter(logs_df.gender == 'F').select('userID').dropDuplicates().count()
print(f"female user count is - {female_user_count}")

# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4
logs_df.filter(logs_df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist': 'count'}) \
    .withColumnRenamed('count(Artist)', 'Playcount') \
    .sort(desc('Playcount')) \
    .show(1)

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: write your code to answer question 5
user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

ishome = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

# Filter only NextSong and Home pages, add 1 for each time they visit Home
# Adding a column called period which is a specific interval between Home visits
cusum = logs_df.filter((logs_df.page == 'NextSong') | (logs_df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', ishome(col('page'))) \
    .withColumn('period', Fsum('homevisit').over(user_window)) 


# This will only show 'Home' in the first several rows due to default sorting

cusum.show(300)


# See how many songs were listened to on average during each period
cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}) \
    .show()
