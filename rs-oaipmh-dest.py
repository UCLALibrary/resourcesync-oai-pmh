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

    for tag in tags:

        # ignore newlines and other whitespace
        if tag.name is None:
            continue

        name = tagNameToColumn[tag.name]
        value = tag.string

        if name in doc:
            if isinstance(doc[name], collections.MutableSequence):
                doc[name].append(value)
            else:
                doc[name] = [doc[name], value]
        else:
            doc[name] = value
    return doc

def main():

    logging.info('--- STARTING SCHEDULED RUN ---')
    logging.info('                              ')

    try:
        solr = pysolr.Solr(args.solrUrl[0])
        open(args.tinydb[0], 'r')
    except:
        logging.critical('Invalid command line arguments')
    else:
        db = TinyDB(args.tinydb[0])
        for row in db:

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
                Row = Query()
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
                            continue

                    elif action == b'updated:':

                        logging.info('Updating Solr document for {}'.format(recordIdentifier))
                        doc = createSolrDoc(recordIdentifier, row['collection_key'], row['institution_key'], tags)
                        try:
                            solr.add([doc])
                        except:
                            logging.error('Something went wrong while trying to send data to Solr')
                            continue

                    elif action == b'deleted:':

                        logging.info('Deleting Solr document for {}'.format(recordIdentifier))
                        try:
                            solr.delete(id=recordIdentifier)
                        except:
                            logging.error('Something went wrong while trying to send data to Solr')
                            continue

    logging.info('                              ')
    logging.info('---  ENDING SCHEDULED RUN  ---\n')

if __name__ == '__main__':
    main()
