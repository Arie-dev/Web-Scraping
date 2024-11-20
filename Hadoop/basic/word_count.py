# # Introduction to Hadoop and Its Components

# Hadoop is an open-source framework that allows for the distributed processing of large datasets across clusters of computers using simple programming models. It is highly scalable, fault-tolerant, and efficient for storing and processing big data.

# In this notebook, we will explore the key components of Hadoop:
# - **HDFS (Hadoop Distributed File System)**: The storage system of Hadoop, designed to handle large files by splitting them into blocks and storing them across multiple nodes.
# - **MapReduce**: A programming model for parallel data processing.
# - **YARN (Yet Another Resource Negotiator)**: Manages resources in a Hadoop cluster and coordinates the execution of tasks.


# # MapReduce Example: Word Count

# MapReduce is the core component of Hadoop for processing large datasets. It works by breaking down tasks into smaller subtasks that can be processed in parallel. The basic MapReduce process includes two phases:
# 1. **Map phase**: Processes the input data and outputs key-value pairs.
# 2. **Reduce phase**: Aggregates the intermediate results and outputs the final data.

# Let's implement a basic MapReduce job in Python using the `mrjob` library to count word frequencies in a text.

from mrjob.job import MRJob

class MRWordCount(MRJob):
    # Mapper function
    def mapper(self, _, line):
        for word in line.split():
            yield (word.lower(), 1)

    # Reducer function
    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    MRWordCount.run()
    
    

# Hadoop and MapReduce:

# MapReduce in Hadoop is a powerful way to process large amounts of data in parallel. Hadoop's MapReduce system handles distributed computing tasks and ensures that the process scales across multiple machines in a cluster. The mrjob library simplifies this process by allowing you to write MapReduce jobs in Python, which can be executed both locally and on a Hadoop cluster.