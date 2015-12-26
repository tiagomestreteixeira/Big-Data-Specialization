# Resilient Distributed Datasets

#Initialize Spark console
> PYSPARK_DRIVER_PYTHON = ipython pyspark

# verify pyspark version
In [1]: sc.version
Out[1]: u'1.3.0'

# Takes a list and distributes it accross 3 partitions
# Returns a reference to RDD
In [3]: integer_RDD = sc.parallelize(range(10),3)

# Collect data back to the driver program (generally done when completed all calculations)
# This data needs to fit in memory
In [6]: integer_RDD.collect()
Out[6]: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


# Show how the data is partionated across the cluster of nodes
# Retrieve data by partitions 
In [7]: integer_RDD.glom().collect()
Out[7]: [[0, 1, 2], [3, 4, 5], [6, 7, 8, 9]]


# Read text into Spark

# from local filesystem
In [11]: text_RDD = sc.textFile("file:///home/cloudera/testfile1")
# outputs the first line
In [12]: text_RDD.take(1)
Out[12]: [u'Hello world in HDFS']


# from HDFS
In [13]: text_RDD = sc.textFile("/user/cloudera/input/testfile1")
# outputs the first line
In [14]: text_RDD.take(1)
Out[14]: [u'Hello world in HDFS']


### Wordcount in Spark: Map


# split each line into words
# returns a list of words
def split_words(line):
	return line.split()

# create key-value pair with value 1
def create_pair(word):
	return (word,1)

pairs_RDD = text_RDD.flatMap(split_words).map(create_pair)

In [15]: pairs_RDD.collect()
Out[15]: 
[(u'A', 1),
 (u'long', 1),
 (u'time', 1),
 (u'in', 1),
 (u'a', 1),
 (u'galaxy', 1),
 (u'far', 1),
 (u'far', 1),
 (u'away', 1)]


### Wordcount in Spark: Reduce


def sum_counts(a,b):
	return a+b

wordcounts_RDD = pairs_RDD.reduceByKey(sum_counts)

In [15]: wordcounts_RDD.collect()
Out[15]: 
[(u'A', 1),
 (u'a', 1),
 (u'far', 2),
 (u'away', 1),
 (u'in', 1),
 (u'long', 1),
 (u'time', 1),
 (u'galaxy', 1)]

