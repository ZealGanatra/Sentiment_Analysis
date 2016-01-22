from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
from pyspark.mllib.recommendation import *
import random

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    plist=[]
    nlist=[]
    
    for count in counts:
	for tup in count:
		if tup[0]=="positive":
			plist.append(int(tup[1]))
		else:
			nlist.append(int(tup[1]))	
    x = range(0,len(plist)) 
    plt.plot(x,plist)
    plt.plot(x,nlist)
    plt.ylabel("word count")
    plt.xlabel("Timestamp")
    plt.show()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    l =[]
    file = open(filename,'rU')
    for line in filename:
	l.append(line)
    return (l)

def word_mapping(word,count,pwords,nwords):
    if word in pwords:
	return("positive",count)
    elif word in nwords:
	return("negative",count)
    return ("null",count)

def updateFunction(newValues, runningCount):
    if runningCount is None:
	runningCount = 0
    return sum(newValues,runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore")) #is a DStream function like RDD

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    #tweets.pprint()
    tweetwords = tweets.flatMap(lambda line:line.split(" "))
    tweetpairs = tweetwords.map(lambda word:(word, 1))
    #tweetpairs.pprint()
    reducedtweetwords = tweetpairs.reduceByKey(lambda x,y: x+y).map(lambda x:word_mapping(x[0],x[1],pwords,nwords))#(word,count) rdd
    #reducedtweetwords.pprint()
    tweetcounts = reducedtweetwords.reduceByKey(lambda x,y:x+y)
    display = []
    t=tweetcounts.filter(lambda x: x[0]!="null")
    t.pprint()
    #t.map(lambda x:display.append(x))	
    
    #twords=tweets.map(lambda x: x.towords(x)).map(lambda y:rateword(y,pwords,nwords))

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    t.updateStateByKey(updateFunction)
    t.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    #counts.append(display)
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
