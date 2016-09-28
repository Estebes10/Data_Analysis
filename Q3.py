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
            #print("Ignoring malformed tweet", line, file=sys.stderr)
            print("Ignored malformed tweet")


def process_tweets_file(filename):
    print("Reading file", filename)
    with gzip.open(filename, "rt") as file:
        map_result = []
        lines = filter(lambda l: l != "", map(str.strip, file.readlines()))
        for tweet in get_tweets(lines):
            map_result.append(extract_hashtags(tweet))
        return map_result
        # return reduce(reduce_function, map_result, reduce_init)


def extract_hashtags(tweet):
    entities = tweet["entities"]
    text = tweet["text"]
    fecha = tweet["created_at"]
    dia = fecha[4:10]
    inhashtag = False
    inmention = False
    intext = False
    ensemble = None
    #if  tweet["retweeted"] is False:
    if "RT " not in text:
        if "Energy" in text or "Aviation" in text or "GeneralElectric" in text: #La gente habla de cosas reelevantes para GE
            intext = True
        hashtags = entities["hashtags"]
        mentions = entities["user_mentions"]
        if len(hashtags) > 0: #La gente cita a GE en sus hashtags
            for tweet2 in hashtags:
                hash = tweet2["text"]
                if "GE_GasPower" in hash or "GEAviation" in hash or "GEHealthcare" in hash or "GeneralElectric" in hash:
                    inhashtag = True
        if len(mentions) > 0:
            for tweet2 in mentions:
                mention = tweet2["screen_name"]
                if "GE_GasPower" in mention or "GEAviation" in mention or "GEHealthcare" in mention or "GeneralElectric" in mention:
                    inmention = True
        if inhashtag == True or inmention == True or intext == True:
            ensemble = {"Date" : dia, "inhashtag" : inhashtag, "inmention" : inmention, "intext" : intext}
            #print(text)
        #print(ensemble)
    return ensemble


if __name__ == "__main__":
    start_time = time()
    data_path = "olympics/"
    files = sorted(os.listdir(data_path))
    n_files = len(files)
    print(n_files, "compressed files.")
    n_read_files = len(files)
    print("Reading: ", n_read_files)
    absolute_paths = list(map(lambda fp: data_path + fp, files))

    # file_map_function = partial(process_tweets_file, extract_hashtags, count_hashtags, {})
    # print(file_map_function)

    with multiprocessing.Pool() as pool:
        jsonF = []
        tweet_entities_per_file = pool.map(process_tweets_file, absolute_paths[:n_read_files])
        for i in tweet_entities_per_file:
            for j in i:
                if j is not None:
                    jsonF.append(j)
        with gzip.open("results/RelevanceOfGEonTweets.json.gz", "wt") as f:
            json.dump(jsonF, fp = f)
'''
    tweet_entities = reduce(consolidate_count_langs, tweet_langs_per_file, {})
    print(tweet_langs)
    with gzip.open("results/HASTAGS.json.gz", "wt") as f:
        json.dump(tweet_langs, fp=f)
    print("Total # of languages:", len(tweet_langs))
    print_elapsed_time(start_time, time())'''