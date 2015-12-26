
def split_words(line):
	return line.split()

def create_pair(word):
	return (word, 1)


pairs_RDD = text_RDD.flatMap(split_words).map(create_pair)

pairs_RDD.collect()


for k,v in pairs_RDD.groupByKey().collect():
    print "Key: ",k, ",Values: ", list(v)

Key:  A ,Values:  [1]
Key:  a ,Values:  [1]
Key:  far ,Values:  [1, 1]
Key:  away ,Values:  [1]
Key:  in ,Values:  [1]
Key:  long ,Values:  [1]
Key:  time ,Values:  [1]
Key:  galaxy ,Values:  [1]


pairs_RDD.cache()


 def sum_counts(a,b):
 	return a + b

 wordcounts_RDD=pairs_RDD.reduceByKey(sum_counts)

wordcounts_RDD.collect()

[(u'A', 1),
 (u'a', 1),
 (u'far', 2),
 (u'away', 1),
 (u'in', 1),
 (u'long', 1),
 (u'time', 1),
 (u'galaxy', 1)]

pairs_RDD.take(1)

[(u'A', 1)]


# Sharing Variables

 config = sc.broadcast({"order":3, "filter":True})
 
 config.value
 {'filter': True, 'order': 3}

# accumulators

 accum = sc.accumulator(0)

 def test_accum(x):
 	accum.add(x)

sc.parallelize([1,2,3,4]).foreach(test_accum)

accum.value
10





