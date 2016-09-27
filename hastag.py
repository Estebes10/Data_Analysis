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

def get_tweets(lines):
    for line in lines:
        try:
            tweet = json.loads(line)
            yield tweet
        except json.JSONDecodeError:
            print("Ignoring malformed tweet", line, file=sys.stderr)

def process_tweets_file(filename):
    print("Reading file", filename)
    with gzip.open(filename, "rt") as file:
        map_result = []
        lines = filter(lambda l: l != "", map(str.strip, file.readlines()))
        for tweet in get_tweets(lines):
            map_result.append(extract_hashtags(tweet))
        return map_result
    #return reduce(reduce_function, map_result, reduce_init)
    
def extract_hashtags(tweet):
    entities = tweet["user"]
    name = entities["screen_name"]
   # hashtag = entities["hashtag"]
   # texto = hashtag["text"]
    return name


if __name__ == "__main__":
    start_time = time()
    data_path = "olympics/"
    files = sorted(os.listdir(data_path))
    n_files = len(files)
    print(n_files, "compressed files.")
    n_read_files = 1
    absolute_paths = list(map(lambda fp: data_path + fp, files))
    
    
    #file_map_function = partial(process_tweets_file, extract_hashtags, count_hashtags, {})
    #print(file_map_function)
 
    with multiprocessing.Pool() as pool:
        tweet_entities_per_file = pool.map(process_tweets_file, absolute_paths[:n_read_files])        
        print(tweet_entities_per_file)
'''
    tweet_entities = reduce(consolidate_count_langs, tweet_langs_per_file, {})
    print(tweet_langs)

    with gzip.open("results/HASTAGS.json.gz", "wt") as f:
        json.dump(tweet_langs, fp=f)

    print("Total # of languages:", len(tweet_langs))
    print_elapsed_time(start_time, time())'''