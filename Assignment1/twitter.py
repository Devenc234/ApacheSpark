from pyspark import SparkContext, SparkConf
sc =SparkContext()

# Input textfile
lines = sc.textFile("/home/zemoso/Desktop/Spark/tweets-1.txt", 1)

# Taking each words on the lines
words = lines.flatMap(lambda x: x.split(' '))

# Filtering the tags
fiterWords = words.filter( lambda x: x.startswith('#'))

# Giving a count of 1 to each tag
ones = fiterWords.map(lambda x: (x, 1))

# Couting all tags
counts = ones.reduceByKey(lambda x, y: x + y)

# Sorting tags according to their count
Toptags = counts.sortBy(lambda x: x[1],False)

# Getting top 100 tags
Top100 = Toptags.take(100)

# Printing top100 tags
Top100
