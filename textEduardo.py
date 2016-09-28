# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 13:29:17 2016
@author: edu
"""

import gzip
import json
import os
import sys
import multiprocessing
from functools import reduce, partial
from time import time
from etime import print_elapsed_time
from _ast import Str
from distutils import text_file
from astropy.io.fits.convenience import append

def search_for_word(tweet, keyword):
    matched_tweet = {};
    #if tweet["retweeted"] == False: #only used if we want just the not retweeted
    country = ["None"]
    username = tweet["user"]["screen_name"]
    createdAt = tweet["created_at"][4:10]
    text = tweet["text"]
    #retweeted = tweet["retweeted"] #not used
    if tweet["place"] is not None:
        country = tweet["place"]["country_code"]
    try:
        if keyword in text:
            matched_tweet = {"country":country, "date":createdAt}
            #matched_tweet.append(str(retweeted))
    except:
        return ""
    return matched_tweet

def get_tweets(lines):
    for line in lines:
        try:
            tweet = json.loads(line)
            yield tweet
        except json.JSONDecodeError:
            print("Ignoring malformed tweet", line, file=sys.stderr)

def process_tweets_file(filename):
    #print("Reading file", filename)
    map_result = []
    with gzip.open(filename, "rt") as file:
        lines = filter(lambda l: l != "", map(str.strip, file.readlines()))
        for tweet in get_tweets(lines):
            map_result.append(extract_text(tweet, "gold"))
            #print(extract_text(tweet, "gold"))
    return map_result
    
def extract_text(tweet, keyword):
    texto = search_for_word(tweet, keyword)
    
    return texto


if __name__ == "__main__":
    start_time = time()
    data_path = "olympics/"
    files = sorted(os.listdir(data_path))
    n_files = len(files)
    print(n_files, "compressed files.")
    n_read_files = n_files
    absolute_paths = list(map(lambda fp: data_path + fp, files))
    
    keyword = "gold"
    
    with multiprocessing.Pool() as pool:
        tweet_entities_per_file = pool.map(process_tweets_file, absolute_paths[:n_read_files])        
    
    final_match_map = []
    #print("len",len(tweet_entities_per_file))
    for item in tweet_entities_per_file:
        final_match = []
        for value in item:
            if value:
               final_match.append(value) 
        final_match_map.append(final_match)
    #print("len:",len(final_match_map))

    jsonElements = []
    for item in final_match_map:
        for element in item:
            jsonElements.append(element)
    #print(jsonElements)
    print("Dumping files...")
    with gzip.open("results/resultsGold.json.gz", "wt") as f:
        json.dump(jsonElements, fp = f)
    print("FINISHED")
#    for value in jsonElements:
        #print(value["text"])
 #       print(value)