#!/usr/bin/python3

# This script is to be run on the ResourceSync destination server.
# It does two main things: 
# - Synchronize our local copy of each collection with updates from member institutions.
# - Propagate these changes to Solr

# Usage: python3 oaipmh-rs.py

import re
import pysolr
import collections
import subprocess
from tinydb import TinyDB, Query
from bs4 import BeautifulSoup
import argparse

parser = argparse.ArgumentParser(description='Synchronize our local copy with updates from member institutions.')
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

        # handle newlines and other whitespace
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

    solr = pysolr.Solr(args.solrUrl)
    db = TinyDB('db.json')

    for collection in db:

        if collection['new'] == True:
            # baseline sync
            command = ['resync', '--baseline', '--verbose', '--delete', '--sitemap', collection['capabilitylist'], collection['urlMapFrom'], collection['filePathMapTo']]
            actions = subprocess.check_output(command, stderr=subprocess.STDOUT)
            print(command)

            # set collection.new = False
            Row = Query()
            db.update({'new': False}, Row['name'] == collection['name'])
        else:
            # incremental sync
            # TODO: currently broken, reliance on --from param
            command = ['resync', '--incremental', '--verbose', '--delete', '--sitemap', collection['capabilitylist'], collection['urlMapFrom'], collection['filePathMapTo']]
            actions = subprocess.check_output(command, stderr=subprocess.STDOUT)
            print(command)

        for line in actions.splitlines():
            action = line.split(b' ')[0]
            if action in [b'created:', b'updated:', b'deleted:']:

                localFile = line.split(b' -> ')[1]
                print(localFile)

                with open(localFile) as fp:
                    soup = BeautifulSoup(fp, 'xml')
                    recordIdentifier = re.sub(r':', '_3A', soup.find('identifier').string)
                    tags = soup.find('dc').contents

                if action == b'created:':
                    print('Creating Solr document for {}'.format(recordIdentifier))
                    doc = createSolrDoc(recordIdentifier, collection['name'], collection['institution'], tags)
                    print(doc)
                    solr.add([doc])

                elif action == b'updated:':
                    print('Updating Solr document for {}'.format(recordIdentifier))
                    doc = createSolrDoc(recordIdentifier, collection['name'], collection['institution'], tags)
                    print(doc)
                    solr.add([doc])

                elif action == b'deleted:':
                    print('Deleting Solr document for {}'.format(recordIdentifier))
                    print(doc)
                    solr.delete(id=recordIdentifier)
                else:
                    print('There was a problem')

if __name__ == '__main__':
    main()
