"""
MAP vs FLATMAP

flatMap takes a line, and expands it out into many rows.

"""
from pyspark import SparkConf, SparkContext
import re

def normalizeWords(text):
    # Remove punctuation, make lowercase, split on whitespace.
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/Rory/repos/SparkCourse/Book.txt")

# Split out into all words
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
