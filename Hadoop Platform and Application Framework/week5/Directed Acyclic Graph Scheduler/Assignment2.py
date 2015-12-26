
hdfs dfs -ls user/cloudera/week5/InputAdvancedJoin/



### Read the shows and views
show_views_file = sc.textFile("/user/cloudera/week5/InputAdvancedJoin/join2_gennum?.txt")

def split_show_views(line):
        line_splited = line.split(",")
        return (line_splited[0],(int)(line_splited[1]))


show_views = show_views_file.map(split_show_views)



### Read the channels and shows
show_channel_file = sc.textFile("/user/cloudera/week5/InputAdvancedJoin/join2_genchan?.txt")

def split_show_channel(line):
        line_splited = line.split(",")
        return (line_splited[0],line_splited[1])

show_channels = show_channel_file.map(split_show_channel)

# Join the two RDD's
joined_dataset = show_views.join(show_channels)
joined_dataset.collect()

### Extract channel as key
def extract_channel_views(show_views_channel):
	show = show_views_channel[0]
	views_channel = show_views_channel[1]
	views = int(views_channel[0])
	channel = views_channel[1]
	return(channel,views)

channel_views = joined_dataset.map(extract_channel_views)
channel_views.collect()


### Sum Across all channels
def some_function(a,b):
	some_result = a + b
	return some_result


channel_views.reduceByKey(some_function).collect()