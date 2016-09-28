import gzip
import json
import os
import sys
import numpy as np
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

map_result = []

def process_tweets_file(tweet_map_function, reduce_function, reduce_init, hashtags, filename):
    print("Reading file", filename)
    print(reduce_function)
    with gzip.open(filename, "rt") as file:
        lines = filter(lambda l: l != "", map(str.strip, file.readlines()))
        for tweet in get_tweets(lines):
            tweet_map_function(tweet) 
    #print(np.vstack(map_result))
    #final = np.concatenate(map_result, axis=1 )
    #print(map_result)
    return reduce(reduce_function, map_result, reduce_init)
    
def extract_hashtags(tweet):
    entities = tweet["entities"]
 #   name = entities["screen_name"]
    #date = tweet["created_at"]
    
    hashtag = entities["user_mentions"]
    if hashtag:
        for t in hashtag:
            json = t['screen_name']
            #print(json)
            map_result.append(json)
    

def count_elements(element_dict, element):
    #print(element_dict)    
    #print(element)    
    if element is not None:
        if element in element_dict.keys():
            element_dict[element] += 1            
    return element_dict


def consolidate_counts(count_dict_acc, element_dict):
    #print(element_dict)
    for tag in element_dict:
        if tag in count_dict_acc:
            count_dict_acc[tag] += element_dict[tag]
        else:
            count_dict_acc[tag] = element_dict[tag]
    return count_dict_acc


if __name__ == "__main__":
    start_time = time()
    data_path = "olympics/"
    files = sorted(os.listdir(data_path))
    n_files = len(files)
    print(n_files, "compressed files.")
    n_read_files = n_files
    absolute_paths = list(map(lambda fp: data_path + fp, files))
    hashtags = {"GEHealthcare":0, "Hitachi_Health": 0, "PhilipsHealth":0, "SiemensHealth":0, "ZiehmImaging":0, "shimadzussi":0, "AtosHealthcare":0}
    
    file_map_function = partial(process_tweets_file, extract_hashtags, count_elements, hashtags, {})
    #print(file_map_function)
 
    with multiprocessing.Pool() as pool:
        tweet_entities_per_file = pool.map(file_map_function, absolute_paths[:n_read_files])        
    
   
    #print(tweet_entities_per_file)

    tweet_entities = reduce(consolidate_counts, tweet_entities_per_file, hashtags)
    #print(tweet_entities)
    
    with gzip.open("results/healthcare.json.gz", "wt") as f:
        json.dump(tweet_entities, fp=f)
    '''
    print("Total # of languages:", len(tweet_langs))'''
    print_elapsed_time(start_time, time())