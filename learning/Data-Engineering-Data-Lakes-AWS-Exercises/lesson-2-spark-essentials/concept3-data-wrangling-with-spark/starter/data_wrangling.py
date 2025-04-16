# Take care of any imports
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import datetime
# Create the Spark Context
spark = SparkSession.builder.appName("Data Wrangling with Spark").getOrCreate()

# Complete the script

path = "/Users/at/Documents/udacity/learning/Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/data/sparkify_log_small.json"

user_log_df = spark.read.json(path)
# # Data Exploration 
# View 5 records 
user_log_df.show(5) 
# Print the schema
user_log_df.printSchema()

# Describe the dataframe
user_log_df.describe().show()

# Describe the statistics for the song length column
user_log_df.describe("length").show()

# Count the rows in the dataframe
print(user_log_df.count())

# Select the page column, drop the duplicates, and sort by page
user_log_df.select("page").dropDuplicates().sort("page").show()

# Select data for all pages where userId is 1046
user_log_df.select(["userId", "firstname", "page", "song"]).where(user_log_df.userId == "1046").show()  

# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))

print(user_log_df.head(1))

# Select just the NextSong page
songs_played = user_log_df.filter(user_log_df.page == "NextSong")
print(user_log_df.head(2))
print(songs_played.count())
# # Drop Rows with Missing Values
songs_played = songs_played.dropna(subset=["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print(songs_played.count())
# select all unique user ids into a dataframe
songs_played = songs_played.select("userId").dropDuplicates()
print(songs_played.count())
# Select only data for where the userId column isn't an empty string (different from null)
songs_played = songs_played.filter(songs_played.userId != "")
print(songs_played.count())
# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 



# Create a user defined function to return a 1 if the record contains a downgrade


# Select data including the user defined function



# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.



# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have



# Show the phases for user 1138 

