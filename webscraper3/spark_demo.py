from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col
import os
import time

spark = SparkSession.builder \
    .master("local") \
    .appName("WordCountSpark") \
    .getOrCreate()

for key, value in os.environ.items():
    if '_HOME' in key:  # Filter for _HOME variables
        print(f"{key} = {value}")

print("SparkSession created successfully")

text_files = []

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

def word_count(file_path):
    try:
        print(f"Processing file: {file_path}")
        
        text_file = spark.read.text(file_path)
        if text_file.count() == 0:
            print(f"Warning: {file_path} is empty.")
            return None
        
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

for idx, file in enumerate(text_files):
    result_df = word_count(file)
    
    if result_df is not None:
        print(f"Word count for {file}:")
        result_df.show()
        
        # Save the DataFrame to a text file
        output_path = os.path.join(output_dir, f"word_count_{idx}.txt")
        with open(output_path, 'w') as f:
            for row in result_df.orderBy('count', ascending=False).collect():
                f.write(f"{row['word']}: {row['count']}\n")
        
        print(f"Results saved to {output_path}")
    else:
        print(f"No result for {file}.\n")

# End the timer
end_time = time.time()

# Print the benchmark result
print(f"Processing time: {end_time - start_time} seconds")
