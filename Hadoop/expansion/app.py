from mrjob.job import MRJob

class MRWordCount(MRJob):
    # Configure the job to allow multiple reducers (default is 2 reducers)
    def configure_args(self):
        # Calling the superclass's configure_args method to ensure that any 
        # built-in arguments are processed
        super(MRWordCount, self).configure_args()
        
        # Add an argument for specifying the number of reducers
        # This is set to 2 by default (i.e., two reducers will process the data).
        self.add_passthru_arg('--num-reducers', type=int, default=2, help='Number of reducers')
    
    # Mapper function
    def mapper(self, _, line):
        """
        This function splits each line of text into words and outputs a key-value pair
        where the key is the word (converted to lowercase) and the value is 1 (indicating 
        the occurrence of that word in the line).
        """
        for word in line.split():
            # Yield each word in lowercase as a key and the value 1 (count for that word)
            yield (word.lower(), 1)
    
    # Reducer function
    def reducer(self, key, values):
        """
        This function is called for each unique word (key).
        It sums the values (the count of occurrences of each word across all the input lines)
        and yields the word along with the total count.
        """
        # Sum the values (i.e., occurrences of the word) and yield the word with the total count
        yield (key, sum(values))

if __name__ == '__main__':
    # Run the job (this runs the MapReduce process: mapping, shuffling, reducing)
    MRWordCount.run()
