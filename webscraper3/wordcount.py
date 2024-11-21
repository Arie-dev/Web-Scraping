from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col
import os
import time
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

# create the sparksession 
# it is the entry point to use the spark dataframe API
# the difference here is we tell it to run on all cpu cores available [*]
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("WordCountSpark") \
    .getOrCreate()

# Start timer
start_time = time.time()

data_folder = os.path.join(os.getcwd(), 'data')
output_dir = os.path.join(os.getcwd(), "output")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# we read all the text files in the data folder
text_files = os.path.join(data_folder, "*.txt")
text_df = spark.read.text(text_files)

# Perform the word count
word_counts_df = text_df.select(explode(split(lower(col("value")), r'\s+')).alias("word")) \
    .groupBy("word").count() \
    .orderBy("count", ascending=False)

# Save the results to the output directory
output_path = os.path.join(output_dir, "word_count")
word_counts_df.write.mode("overwrite").csv(output_path)

# End timer and print the processing time
end_time = time.time()
print(f"Processing completed in {end_time - start_time:.2f} seconds.")
print(f"Results saved to {output_path}")

# Stop SparkSession
spark.stop()