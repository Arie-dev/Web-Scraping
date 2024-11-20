### Word Count with MapReduce in Hadoop using `mrjob`
```bash
pip install mrjob
```


### Introduction

In Hadoop, **MapReduce** is a programming model used to process large datasets in parallel. The process involves two key phases:

1. **Map Phase**: This phase processes input data and generates intermediate key-value pairs.
2. **Reduce Phase**: The intermediate data is grouped by key, and each group is processed by a reducer to produce the final result.

In this example, we are using `mrjob`, a Python library that simplifies writing Hadoop MapReduce jobs. We'll write a Word Count job, where the task is to count the frequency of each word in a text file.

### Components of Hadoop

Before diving into the code, let's quickly review the main components of Hadoop:

- **HDFS (Hadoop Distributed File System)**: The storage layer of Hadoop. It stores large files across multiple nodes in a cluster, splitting them into smaller blocks for efficient storage and retrieval.
- **MapReduce**: The processing layer of Hadoop. It splits large tasks into smaller tasks (mappers) that are processed in parallel, and the results are combined by reducers.
