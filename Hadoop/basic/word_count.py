from mrjob.job import MRJob

class MRWordCount(MRJob):
    # """Mapper function"""
    # The mapper function is responsible for processing each input line and
    # generating key-value pairs. In this case, each word in the line is
    # paired with the value 1, representing its occurrence.
    def mapper(self, _, line):
        # Split the line into words and process each word
        for word in line.split():
            """Yield the word in lowercase as a key and the value 1 (count for that word)"""
            # Yielding the word as a key and 1 as the count for the word
            yield (word.lower(), 1)

    # """Reducer function"""
    # The reducer function takes the key-value pairs emitted by the mapper and 
    # processes them. For each unique word (key), it sums the counts (values)
    # to get the total occurrences of that word.
    def reducer(self, key, values):
        """Sum the values (i.e., occurrences of the word) and yield the word with the total count"""
        # Sum all the occurrences (values) of the word and output the total count
        yield (key, sum(values))

if __name__ == '__main__':
    
    # """Run the job (this runs the MapReduce process: mapping, shuffling, reducing)"""
    # This is where the job is executed. The MRJob framework will handle the 
    # orchestration of the Map and Reduce phases, managing the distribution 
    # of tasks, data, and resources.
    MRWordCount.run()

"""py word_count.py input.txt --output-dir=output """