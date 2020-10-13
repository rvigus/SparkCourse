"""
Filter() removes data from your RDD.

Filters on whether a condition is met.
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])



"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/Rory/repos/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)

# Filter out everything that is not TMIN.
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# Create new key, value using station_id and temperature
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# Find the minimum temperature observed for every stationid
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect()
for result in results:
    print(result[0] + "\t{:.2f}c".format(result[1]))
