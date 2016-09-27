import gzip
import json
import os
import sys
import multiprocessing
from functools import reduce, partial
from time import time
from etime import print_elapsed_time


def get_tweets(lines):
    for line in filter(lambda l: l != "", map(str.strip, lines)):
        try:
            tweet = json.loads(line)
            yield tweet
        except json.JSONDecodeError:
            print("Ignoring malformed tweet", line, file=sys.stderr)


def process_tweets_file(tweet_map_function, reduce_function, reduce_init, filter_function, filename):
    print("Reading file", filename)
    with gzip.open(filename, "rt") as file:
        map_result = []
        for tweet in filter(filter_function, get_tweets(file.readlines())):
            map_result.append(tweet_map_function(tweet))
    return reduce(reduce_function, map_result, reduce_init)


def filter_lang(target_lang, tweet):
    satisfy = False
    if tweet["entities"] is not None:
        satisfy = (tweet["entities"] == target_lang)
    return satisfy
    
'''
def extract_country(tweet):
    country = "None"
    place = tweet["place"]
    if place is not None:
        country = place["country_code"]
    return country
'''

def extract_hashtag(tweet):
    line_text = "None"
    entitie = tweet["entities"]
    if entitie is not None:
        line_text = entitie["hashtags"]
        final_text = line_text["text"]
    return final_text

def count_elements(element_dict, element):
    if element in element_dict.keys():
        element_dict[element] += 1
    else:
        element_dict[element] = 1
    return element_dict


def consolidate_counts(count_dict_acc, element_dict):
    for final_text in element_dict.keys():
        if final_text in count_dict_acc.keys():
            count_dict_acc[final_text] += element_dict[final_text]
        else:
            count_dict_acc[final_text] = element_dict[final_text]
    return count_dict_acc


if __name__ == "__main__":
    start_time = time()
    data_path = "olympics/"
    files = sorted(os.listdir(data_path))
    n_files = 10
    print(n_files, "compressed files.")
    n_read_files = n_files
    absolute_paths = list(map(lambda fp: data_path + fp, files))

    lang = "en"
    filter_lang_eng = partial(filter_lang, lang)
    file_map_function = partial(process_tweets_file, extract_hashtag, count_elements, {}, filter_lang_eng)

    with multiprocessing.Pool() as pool:
        hashtag_per_file = pool.map(file_map_function, absolute_paths[:n_read_files])

    """hashtags_reduce = reduce(consolidate_counts, hashtag_per_file, {})
    ensemble = {"hashtag": hashtags_reduce}
    """
    print(hashtag_per_file)
    
"""
    with gzip.open("results/lang_countries." + str(n_read_files) + ".json.gz", "wt") as f:
        json.dump(ensemble, fp=f)

    print("Total # of countries:", len(lang_countries))
    print_elapsed_time(start_time, time())
"""