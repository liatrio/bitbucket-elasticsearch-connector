#!/usr/bin/env python
# -*- coding: utf-8 -*-

from index import index_repos
import logging
import os
import time
import requests
import json
import getpass
import sys, signal
import atexit

try:
    from argparse import ArgumentParser
except ImportError:
    logging.error("argparse is required to run this script")
    exit(1)
try:
    from elasticsearch import Elasticsearch
except ImportError:
    logging.error("Elasticsearch is required to run this script")
    exit(1)
try:
    from requests_oauthlib import OAuth1
except ImportError:
    logging.error("requests-oauthlib is required to run this script")
    exit(1)

lastrun = None

def check_es_configs(config):
    if 'host' not in config.keys():
        raise KeyError("Elasticsearch host is missing in elasticsearch.conf")
        exit(1)
    if 'repo_index' not in config.keys():
        raise KeyError("Elasticsearch repo_index is missing in elasticsearch.conf")
        exit(1)
    if 'file_index' not in config.keys():
        raise KeyError("Elasticsearch file_index is missing in elasticsearch.conf")
        exit(1)
    if 'commit_index' not in config.keys():
        raise KeyError("Elasticsearch commit_index is missing in elasticsearch.conf")
        exit(1)

def check_bitbucket_configs(config):
    if 'token' not in config.keys():
        raise KeyError("Bitbucket token is missing in bitbucket.conf")
        exit(1)
    if 'api_endpoint' not in config.keys():
        raise KeyError("Bitbucket api_endpoint is missing in bitbucket.conf")
        exit(1)

def last_run():
    '''
    reads from .bitbucketHistory when bitbucket content was last indexed
    '''
    if os.path.isfile(".bitbucketHistory"):
        sincestr = open(".bitbucketHistory").read()
        since = time.strptime(sincestr, '%Y-%m-%dT%H:%M:%S')
    else:
        since = 0
    return since

def write_history(lastrun):
    '''
    writes the timestamp when bitbucket content was last indexed or updated
    uses a file named '.bitbucketHistory' to save the timestamp for next run
    '''
    if lastrun:
        history_file = open(".bitbucketHistory", 'w')
        history_file.write(lastrun)
        history_file.close()

def init_elasticsearch():
    config = {}
    execfile("elasticsearch.conf", config)
    check_es_configs(config)
    try:
        es = Elasticsearch(config['host'], max_retries=8)
    except:
        logging.error("elasticsearch is not running")
        exit(1)
    if not es.indices.exists(index=config['repo_index']):
        es.indices.create(index=config['repo_index'])
    if not es.indices.exists(index=config['file_index']):
        es.indices.create(index=config['file_index'])
    if not es.indices.exists(index=config['commit_index']):
        es.indices.create(index=config['commit_index'])
        commit_mapping = json.loads(open("commit_mapping.json", "r").read())
        es.indices.put_mapping(index=config['commit_index'], doc_type='_doc', body=commit_mapping)
    return es


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    bitbucket_config = {}
    execfile("bitbucket.conf", bitbucket_config)
    check_bitbucket_configs(bitbucket_config)
    headers = {"Authorization":"Bearer "+bitbucket_config['token']}

    ## Bitbucket connection:
    s = requests.Session()
    s.headers = headers

    ## elasticsearch connection:
    es_conn = init_elasticsearch()

    argparser = ArgumentParser(description=__doc__)
    argparser.add_argument('index', default='index',
                           help='index, update or pindex')
    args = argparser.parse_args()
    if args.index == "index":
        lastrun = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        index_repos(s, es_conn)
        write_history(lastrun)
    else:
        raise ValueError("Unknown mode. Please use one of the following:\n index")

atexit.register(write_history, lastrun)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        #lastrun = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())
        print lastrun
        write_history(lastrun)    
        sys.exit()
