"""
KEY/VALUE RDD's

RDD's can hold key/value pairs.
Key is age, value is number of friends.
store (age:value), (age:value) in the RDD

totalsByAge = rdd.map(lambda x: (x, 1))


reduceByKey(): combines values with the same key using some function.
rdd.reduceByKey(lambda x, y: x + y) adds them up.
groupByKey(): Group values with the same key
sortBykey(): Sort RDD by key values
keys(), values(): Create an RDD out of just the keys, or just the values.


# joining different RDD's on keys
join, rightOuterJoin, leftOuterJoin, cogroup, subtractByKey()

"""
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# id, name, age, n_friends
lines = sc.textFile("file:///Users/Rory/repos/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)

# mapValues (33, 385) -> (33, (385, 1))
# (33, 2) -> (33, (2, 1))
# We need the total number of friends, and the total number of times an age was seen.
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# How do we combine things for the same key? (add up friends, add up n_times_seen)
# (33, (387, 2)
# reduceBykey(lambda x,y: (x[0] + y[0], x[1] + y[1]))

# For each row, divide the two elements to get average_friends_by_age
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averagesByAge.collect()
for result in results:
    print(result)
