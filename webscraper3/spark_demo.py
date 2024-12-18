from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col
import os
import time


#create the spark session, this is the entry point for an apache spark application
# master is going to run spark locally on one core (default)
spark = SparkSession.builder \
    .master("local") \
    .appName("WordCountSpark") \
    .getOrCreate()

# debugging info for troubleshooting apache spark installation
for key, value in os.environ.items():
    if '_HOME' in key:
        print(f"{key} = {value}")

print("SparkSession created successfully")


# get the path of the data we will parse
data_folder = os.path.join(os.getcwd(), 'webscraper3/data')
text_files = [os.path.join(data_folder, "file1.txt"),
              os.path.join(data_folder, "file2.txt"),
              os.path.join(data_folder, "file3.txt")]

# data_folder = os.path.join(os.getcwd(), 'webscraper3/data')
# text_files = []

# for file_name in os.listdir(data_folder):
#     if file_name.endswith('.txt'):
#         text_files.append(os.path.join(data_folder, file_name))


output_dir = os.path.join(os.getcwd(), "output")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# define the word_count functions
def word_count(file_path):
    try:
        print(f"Processing file: {file_path}")
        
        # use the apache spark read.text to read the text in the file
        text_file = spark.read.text(file_path)
        if text_file.count() == 0:
            print(f"Warning: {file_path} is empty.")
            return None
        
        # then we create a dataframe from this
        # explode will put each word into a seperate row
        # split words by whitespace
        # alias renames the column to word
        # groupby word and then count to get a total count of how many times this word occurs
        # order by count in descending order
        words_df = text_file.select(explode(split(lower(text_file['value']), r'\s+')).alias('word'))

        word_counts_df = words_df.groupBy('word').count().orderBy('count', ascending=False)

        if word_counts_df.count() == 0:
            print(f"No words found in {file_path}.")
            return None

        return word_counts_df

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


start_time = time.time()

# then for the main program
for idx, file in enumerate(text_files):
    # for every file we call that function
    result_df = word_count(file) 
    
    if result_df is not None:
        print(f"Word count for {file}:")
        result_df.show()
        # we show the head for every file counted in between to see progress
        
        # save the final results to a text file per fle
        output_path = os.path.join(output_dir, f"word_count_{idx}.txt")
        with open(output_path, 'w') as f:
            for row in result_df.orderBy('count', ascending=False).collect():
                f.write(f"{row['word']}: {row['count']}\n")
        
        print(f"Results saved to {output_path}")
    else:
        print(f"No result for {file}.\n")


end_time = time.time()

print(f"Processing time: {end_time - start_time} seconds")
