#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import time
import requests
import json
from requests.exceptions import ConnectionError

try:
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
except ImportError:
    logging.error("Elasticsearch is required to run this script")
    exit(1)

es_config = {}
bb_config = {}
execfile("elasticsearch.conf", es_config)
execfile("bitbucket.conf", bb_config)

bb_projects_url = bb_config['api_endpoint'] + "/projects"
bb_repos_url_slug = "/repos"
bb_branches_url_slug = "/branches"
bb_browse_url_slug = "/browse"
bb_commits_url_slug = "/commits"

def fetch_branches(session, repo):
    '''
    For a set of repositories, update the repo with a list of all its branches
    '''
    branches = []
    api_params = ''
    while True:
        bb_branches_url = bb_projects_url+"/"+repo['project']['key']+bb_repos_url_slug+"/"+repo['slug']+bb_branches_url_slug
        try:
            response = session.get(bb_branches_url, params=api_params)
        except ConnectionError:
            logging.error("Connection error")
            exit(1)
        if response.status_code == requests.codes.ok:
            if int(response.json()['size']) < 1:
                break
            for branch in response.json()['values']:
                branches.append(branch)
            if bool(response.json()['isLastPage']):
                break
            else:
                api_params = {"start": response.json()['nextPageStart']}
        else:
            logging.info("Fetching branches stopped with response code " + str(response.status_code))
            exit(1)
    repo.update({unicode("branches") : branches})
    return repo

def fetch_projects(session, es):
    '''
    Gets the list of all projects in the Bitbucket instance
    '''
    projects = []
    api_params = ''
    while True:
        try:
            response = session.get(bb_projects_url, params=api_params)
        except ConnectionError:
            logging.error("Connection error")
            exit(1)
        if response.status_code == requests.codes.ok:
            if int(response.json()['size']) < 1:
                break
            for project in response.json()['values']:
                projects.append(project)
            if bool(response.json()['isLastPage']):
                break
            else:
                api_params = {"start": response.json()['nextPageStart']}
        else:
            logging.info("Fetching projects stopped with response code " + str(response.status_code))
            exit(1)
    return projects

def index_repos(session, es):
    '''
    Process all the repos
    '''
    logging.info("Indexing repositories")
    for project in fetch_projects(session, es):
        logging.info("Processing project" + project['name'])
        api_params = ''
        while True:
            bb_repos_url = bb_projects_url+"/"+project['key']+bb_repos_url_slug
            try:
                response = session.get(bb_repos_url, params=api_params)
            except ConnectionError:
                logging.error("Connection error")
                exit(1)
            if response.status_code == requests.codes.ok:
                if int(response.json()['size']) < 1:
                    break
                for repo in response.json()['values']:
                    repo = fetch_branches(session, repo)
                    repo_search = es.search(index=es_config['repo_index'], body={"query":{ "match_phrase":{"id": repo['id'] }}})
                    if len(repo_search['hits']['hits']) > 0:
                        es.index(index=es_config['repo_index'], doc_type="_doc", id=repo_search['hits']['hits'][0]['_id'], body=repo)
                    else:
                        es.index(index=es_config['repo_index'], doc_type="_doc", body=repo)
                    es.indices.refresh(index=es_config['repo_index'])
                    process_branch(session, es, repo)
                if bool(response.json()['isLastPage']):
                    break
                else:
                    api_params = {"start": response.json()['nextPageStart']}
            else:
                logging.info("Indexing repos stopped with response code " + str(response.status_code))
                exit(1)

def process_branch(session, es, repo):
    '''
    Given a repo, index the files and commits in each branch
    '''
    for branch in repo['branches']:
        logging.info("Started indexing: " + repo['project']['key']+"/"+repo['slug']+"@"+branch['displayId'])
        bb_files_url = bb_projects_url+"/"+repo['project']['key']+bb_repos_url_slug+"/"+repo['slug']+bb_browse_url_slug
        bb_commits_url = bb_projects_url+"/"+repo['project']['key']+bb_repos_url_slug+"/"+repo['slug']+bb_commits_url_slug
        bulk_files_to_index = []
        bulk_commits_to_index = []
        es.delete_by_query(index=es_config['file_index'], body={"query": { "bool": { "must": { "match" : { "project_name": repo['project']['key'] }, "match" : { "repo_name": repo['slug'] }, "match" : { "branch.displayId": branch['displayId'] }}}} })
        index_dir(session, es, repo, branch, bb_files_url, bulk_files_to_index)
        if len(bulk_files_to_index) > 0:
            logging.info(str(len(bulk_files_to_index)) + " files will be indexed")
            helpers.bulk(es, bulk_files_to_index)
            es.indices.refresh(index=es_config['file_index'])
        else:
            logging.info("Zero files were returned for this branch.")
        index_commits(session, es, repo, branch, bb_commits_url, bulk_commits_to_index)
        if len(bulk_commits_to_index) > 0:
            logging.info(str(len(bulk_commits_to_index)) + " commits will be indexed")
            helpers.bulk(es, bulk_commits_to_index)
            es.indices.refresh(index=es_config['commit_index'])
        else:
            logging.info("Zero new commits were returned for this branch.")
                
def index_dir(session, es, repo, branch, bb_files_url, bulk_index):
    '''
    recursively indexes a given repo's files in a given branch
    '''
    api_params = ''
    while True:
        try:
            response = session.get(bb_files_url+"?at="+branch['displayId'], params=api_params)
        except ConnectionError:
            logging.error("Connection error")
            exit(1)
        if response.status_code == requests.codes.ok:
            if int(response.json()['children']['size']) < 1:
                break
            for child in response.json()['children']['values']:
                if child['type'] == 'DIRECTORY':
                    index_dir(session, es, repo, branch, bb_files_url+"/"+child['path']['toString'], bulk_index)
                child['path'].update({unicode("parent"): response.json()['path']['toString']})
                child.update({unicode("repo_name"): repo['slug']})
                child.update({unicode("project_key"): repo['project']['key']})
                child.update({unicode("branch"): branch})
                action = {}
                action.update({"_source": child})
                action.update({"_index" : es_config['file_index']})
                action.update({"_type" : '_doc'})
                bulk_index.append(action)
            if bool(response.json()['children']['isLastPage']):
                break
            else:
                api_params = {"start": response.json()['children']['nextPageStart']}
        else:
            logging.info("Indexing files stopped with response code " + str(response.status_code))
            exit(1)

def index_commits(session, es, repo, branch, bb_commits_url, bulk_index):
    '''
    Given a repo/branch, index the commits
    '''
    api_params = ''
    while True:
        try:
            response = session.get(bb_commits_url, params=api_params)
        except ConnectionError:
            logging.error("Connection error")
            exit(1)
        if response.status_code == requests.codes.ok:
            if int(response.json()['size']) < 1:
                break
            for commit in response.json()['values']:
                commit_search = es.search(index=es_config['commit_index'], body={"query":{ "match":{ "id": commit['id']}}})
                if len(commit_search['hits']['hits']) > 0:
                    break
                commit.update({unicode("repo_name"): repo['slug']})
                commit.update({unicode("project_key"): repo['project']['key']})
                commit.update({unicode("branch"): branch})
                action = {}
                action.update({"_source": commit})
                action.update({"_index" : es_config['commit_index']})
                action.update({"_type" : '_doc'})
                bulk_index.append(action)
            if bool(response.json()['isLastPage']):
                break
            else:
                api_params = {"start": response.json()['nextPageStart']}
        else:
            logging.info("Indexing commits stopped with response code " + str(response.status_code))
            exit(1)
                
