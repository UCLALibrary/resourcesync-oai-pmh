#!/usr/bin/python3

# rs-oaipmh-dest.py
#
# This script is to be run on the ResourceSync destination server.
# It does two main things: 
# - Synchronize our local copy of each collection with updates from member institutions.
# - Propagate these changes to Solr

import re
import pysolr
import collections
import subprocess
from tinydb import TinyDB, Query
from bs4 import BeautifulSoup
import argparse
import logging
import sys
from dateutil.parser import parse
from datetime import date
import validators

def addValuePossiblyDuplicateKey(key, value, dic):
    if key in dic:
        if isinstance(dic[key], collections.MutableSequence):
            dic[key].append(value)
        else:
            dic[key] = [dic[key], value]
    else:
        dic[key] = value

tagNameToColumn = {
    'title': 'title_keyword',
    'creator': 'creator_keyword',
    'subject': 'subject_keyword',
    'description': 'description_keyword',
    'publisher': 'publisher_keyword',
    'contributor': 'contributor_keyword',
    'date': 'date_keyword',
    'type': 'type_keyword',
    'format': 'format_keyword',
    'identifier': 'identifier_keyword',
    'source': 'source_keyword',
    'language': 'language_keyword',
    'relation': 'relation_keyword',
    'coverage': 'coverage_keyword',
    'rights': 'rights_keyword'
    }

def createSolrDoc(identifier, colname, instname, tags):

    doc = {
        'id': identifier,
        'collectionName': colname,
        'institutionName': instname
    }
    dates = set()

    for tag in tags:

        # ignore newlines and other whitespace
        if tag.name is None:
            continue

        name = tagNameToColumn[tag.name]
        value = tag.string

        addValuePossiblyDuplicateKey(name, value, doc)

        if tag.name == 'date':
            dates = dates | cleanAndNormalizeDate(value)

    for decade in facet_decades(dates):
        addValuePossiblyDuplicateKey('decade', decade, doc)

    return doc

def facet_decades(years):
    '''Returns a set of decades that spans all of the years in "years".'''

    currentYear = date.today().year

    # matches = set(filter(lambda a: a >= 1000, years))
    matches = set(filter(lambda a: a <= currentYear, years))
    if len(matches) == 0:
        return {}

    start = min(matches) // 10 * 10
    end = max(matches) + 1
    return set(range(start, end, 10))

def cleanAndNormalizeDate(dateString):
    '''Returns a normalized set of years found in the dateString.'''

    # TODO: use this array to generate regex pattern
    digitPlaceholders = [
        '-',
        '?',
        '*'
        ]
    # First, see if dateutil can parse the date string
    try:
        return {parse(dateString).year}

    # If not, find as many substrings that look like years as possible
    # We'll permit the one's place to be unknown
    except ValueError:
        pattern = re.compile(r'(?<!\d)(\d{3}[-*?0-9])(?!\d)')
        matches = re.findall(pattern, dateString)
        
        if len(matches) > 0:
            # If the one's place is unknown, just round down to the nearest decade
            return {int(m) if re.compile('\d{4}').match(m) is not None else int(m[:3] + '0') for m in matches}
        else:
            # search for years of length three
            pattern = re.compile(r'(?<!\d)(\d{2}[-*?0-9])(?!\d)')
            matches = re.findall(pattern, dateString)
            
            if len(matches) > 0:
                # If the one's place is unknown, just round down to the nearest decade
                return {int(m) if re.compile('\d{3}').match(m) is not None else int(m[:2] + '0') for m in matches}
            else:
                # search for patterns of length two
                pattern = re.compile(r'(?<!\d)(\d[-*?0-9])(?!\d)')
                matches = re.findall(pattern, dateString)
                
                if len(matches) > 0:
                    # If the one's place is unknown, just round down to the nearest decade
                    return {int(m) if re.compile('\d{2}').match(m) is not None else int(m[:1] + '0') for m in matches}
                else:
                    # search for years of length one
                    pattern = re.compile(r'(?<!\d)(\d)(?!\d)')
                    matches = re.findall(pattern, dateString)
                    
                    if len(matches) > 0:
                        # If the one's place is unknown, just round down to the nearest decade
                        return {int(m) for m in matches}
                    else:
                        # error
                        return {}

def main():

    logging.basicConfig(
        level=logging.INFO,
        filename='rs-oaipmh-dest.log',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p'
        )

    parser = argparse.ArgumentParser(description='Synchronize our local copy with updates from member institutions.')
    parser.add_argument('tinydb', metavar='TINYDB', nargs=1, help='Path to the local TinyDB instance.')
    parser.add_argument('solrUrl', metavar='SOLR', nargs=1, help='Base URL of Solr index.')
    args = parser.parse_args()

    logging.info('--- STARTING SCHEDULED RUN ---')
    logging.info('                              ')

    # make sure URL is well-formed
    if not validators.url(args.solrUrl[0]):
        logging.critical('Invalid command line argument: {} is not a valid URL'.format(args.solrUrl[0]))
        exit(1)

    try:
        # throws exception if the file doesn't exist
        with open(args.tinydb[0], 'r') as f:
            pass
    except:
        logging.critical('Invalid command line argument: {} does not exist'.format(args.tinydb[0]))
        exit(1)

    solr = pysolr.Solr(args.solrUrl[0])
    db = TinyDB(args.tinydb[0])
    for row in db:

        Row = Query()

        if row['new'] == True:

            # baseline sync
            command = ['resync', '--baseline', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], row['url_map_from'], row['file_path_map_to']]
            try:
                actions = subprocess.run(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True
                    )
            except subprocess.CalledProcessError as e:
                logging.error('Invalid invocation of "resync" with collection {}'.format(row['collection_key']))
                logging.error(e)
                continue

            # set row.new = False
            db.update({'new': False}, Row['collection_key'] == row['collection_key'])

        else:

            # incremental sync
            command = ['resync', '--incremental', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], '--changelist-uri', row['changelist_uri'], row['url_map_from'], row['file_path_map_to']]
            try:
                actions = subprocess.run(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True
                    )
            except subprocess.CalledProcessError as e:
                logging.error('Invalid invocation of "resync" with collection {}'.format(row['collection_key']))
                logging.error(e)
                continue

        for line in actions.stdout.splitlines():

            '''
            # TODO: fault tolerance
            failures = row['_failures'] or []
            '''

            action = line.split(b' ')[0]
            if action in [b'created:', b'updated:', b'deleted:']:

                localFile = line.split(b' -> ')[1]
                logging.debug('Cenerating Solr document from records in "{}"'.format(localFile))

                with open(localFile) as fp:
                    soup = BeautifulSoup(fp, 'xml')
                    recordIdentifier = re.sub(r':', '_3A', soup.find('identifier').string)
                    tags = soup.find('dc').contents

                if action == b'created:':

                    logging.info('Creating Solr document for {}'.format(recordIdentifier))
                    doc = createSolrDoc(recordIdentifier, row['collection_key'], row['institution_key'], tags)
                    try:
                        solr.add([doc])
                    except:
                        logging.error('Something went wrong while trying to send data to Solr')
                        '''
                        # TODO: fault tolerance
                        failures.push({
                            'collection_key': recordIdentifier,
                            'action': action
                            })
                        '''
                        continue

                elif action == b'updated:':

                    logging.info('Updating Solr document for {}'.format(recordIdentifier))
                    doc = createSolrDoc(recordIdentifier, row['collection_key'], row['institution_key'], tags)
                    try:
                        solr.add([doc])
                    except:
                        logging.error('Something went wrong while trying to send data to Solr')
                        '''
                        # TODO: fault tolerance
                        failures.push({
                            'collection_key': recordIdentifier,
                            'action': action
                            })
                        '''
                        continue

                elif action == b'deleted:':

                    logging.info('Deleting Solr document for {}'.format(recordIdentifier))
                    try:
                        solr.delete(id=recordIdentifier)
                    except:
                        logging.error('Something went wrong while trying to send data to Solr')
                        '''
                        # TODO: fault tolerance
                        failures.push({
                            'collection_key': recordIdentifier,
                            'action': action
                            })
                        '''
                        continue

        '''
        # TODO: fault tolerance
        # keep track of any documents that failed to get added to Solr
        db.update({'_failures': failures}, Row['collection_key'] == row['collection_key'])
        # TODO: consider the case where a document is added/updated/deleted from Solr after it fails. Do we do a check each time we do some action to see if it exists in a _failures property?
        '''

    logging.info('                              ')
    logging.info('---  ENDING SCHEDULED RUN  ---\n')

if __name__ == '__main__':
    main()
