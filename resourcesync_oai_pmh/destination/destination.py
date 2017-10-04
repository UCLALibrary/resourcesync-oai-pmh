#!/usr/bin/python3

import boto3
from bs4 import BeautifulSoup
import collections
from configparser import ConfigParser
from datetime import date
from dateutil.parser import parse
from functools import reduce
from json import dumps
import logging
import logging.config
from mimetypes import guess_type, guess_extension
import os
import pathlib
import pysolr
import re
import requests
import subprocess
import sys
from tinydb import TinyDB, Query
import urllib.parse
import validators

from util import DateCleanerAndFaceter, HyperlinkRelevanceHeuristicSorter

'''
# TODO: move everything inside class
class ResourceSyncOAIPMHDestination:
    pass
'''
base_dir = os.path.abspath(os.path.dirname(__file__))

config_path = os.path.join(base_dir, 'destination.ini')
config = ConfigParser()
config.read(config_path)

logging_config_path = os.path.join(base_dir, 'destination_logging.ini')
logging_config = ConfigParser()
logging_config.read(logging_config_path)

logging.config.fileConfig(logging_config_path)
logger = logging.getLogger('root')

s3 = boto3.Session(profile_name=config['S3']['profile_name']).client('s3')

# map from DC tag name to Solr field name
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

# list of BeautifulSoup filters to pass to the find_all function, in order of priority to check
bsFilters = [
    'identifier.thumbnail',
    'identifier',
    re.compile('identifier.*')
    ]


def addValuePossiblyDuplicateKey(key, value, dic):
    '''Adds a key-value pair to a dictionary that may already have a value for that key. If that's the case, put both values into a list. This is how pysolr wants us to represent duplicate fields.'''

    if key in dic:
        if isinstance(dic[key], collections.MutableSequence):
            dic[key].append(value)
        else:
            dic[key] = [dic[key], value]
    else:
        dic[key] = value


def createSolrDoc(identifier, rowInDB, thumbnailurl, tags, hostHeuristic):
    '''Maps a Dublin Core record to a Solr document to be indexed.'''

    doc = {
        'id': identifier,
        'collectionKey': rowInDB['collection_key'],
        'collectionName': rowInDB['collection_name'],
        'institutionKey': rowInDB['institution_key'],
        'institutionName': rowInDB['institution_name']
    }
    if thumbnailurl is not None:
        doc['thumbnail_url'] = thumbnailurl

    years = set()

    # TODO: change to set
    hyperlinks = []

    for tag in tags:

        # ignore newlines and other whitespace in the list of tags
        if tag.name is None:
            continue

        try:
            # only process Dublin Core fields (no qualified DC)
            name = tagNameToColumn[tag.name]
        except KeyError as e:
            continue
        else:
            value = tag.string
            addValuePossiblyDuplicateKey(name, value, doc)

            # build up a set of all the years included in the metadata
            if name == tagNameToColumn['date']:
                years.add(value)
            elif name == tagNameToColumn['title'] and 'first_title' not in doc:
                doc['first_title'] = value
            elif name == tagNameToColumn['identifier'] and validators.url(value) and os.path.splitext(urllib.parse.urlparse(value).path)[1] not in ['.jpg', '.jpeg', '.png', '.tif', '.tiff']:
                hyperlinks.append(value)

    if len(years) > 0:
        decades = DateCleanerAndFaceter(years).decades()

        if len(decades) > 0:
            for decade in decades:
                addValuePossiblyDuplicateKey('decade', decade, doc)
            doc['sort_decade'] = min(decades, key=lambda x: int(x))
            logger.debug('years "{}" -> decades "{}"'.format(years, decades))
    if len(hyperlinks) > 0:
        if isOaiIdentifier(identifier):
            ident = identifier.split(sep=':', maxsplit=2)[2]
        else:
            ident = identifier

        heuristics = {
            'host': hostHeuristic,
            'identifier': ident
        }
        hrhs = HyperlinkRelevanceHeuristicSorter(heuristics, hyperlinks)
        doc['external_link'] = hrhs.mostRelevant()

        rest = hrhs.rest()
        if len(rest) > 0:
            doc['alternate_external_link'] = rest

    return doc


def isOaiIdentifier(identifier):
    '''Return true if the given identifier follows the syntax specified here: http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm.'''

    components = identifier.split(sep=':', maxsplit=2)
    return components[0] == 'oai' and len(components) == 3


def findThumbnailUrl(bs, filters):
    '''Return the URL of the thumbnail for a Dublin Core record. If none exists, return None.

    bs - BeautifulSoup representation of the metadata file
    filters - a list of filters to pass to the find_all function, that denote where a URL might live
    '''

    checkedUrls = []
    for f in filters:
        # search for tags that match the filter (can be regex or string, see )
        tags = bs.find_all(f)

        for tag in tags:
            possibleUrl = tag.string
            if validators.url(possibleUrl) and possibleUrl not in checkedUrls:
                logger.debug('Checking for thumbnail at {}'.format(possibleUrl))
                # TODO: maybe check path extension before doing get request?
                #r = requests.get(possibleUrl)

                resp = makeThumbnailRequest(requests.head, possibleUrl, False, True)

                if resp is not None:
                    try:
                        m = re.search(re.compile('image/(?:jpeg|tiff|png)'), resp.headers['content-type'])
                        logger.debug('Match: {}'.format(m))
                        if m is not None:
                            return resp.url
                        else:
                            checkedUrls.append(possibleUrl)
                    # no content-type
                    except KeyError:
                        checkedUrls.append(possibleUrl)
    return None


def makeThumbnailRequest(fn, url, stream, redirect):
    '''
    Make request to the given URL and handle the response.

    If we can do something with the response, return it, otherwise return None.
    '''
    nTries = 0
    maxTries = 3
    while nTries < maxTries:
        try:
            r = fn(url, stream=stream, timeout=60, allow_redirects=redirect)
            r.raise_for_status()
            break
        except requests.Timeout as e:
            # try a couple more times, server may be restarting
            logger.debug('Trying again...')
            nTries += 1
        except requests.ConnectionError as e:
            return None
        except requests.TooManyRedirects as e:
            return None
        except requests.URLRequired as e:
            return None
        except requests.HTTPError as e:
            return None
        except requests.RequestException as e:
            return None

    if nTries == maxTries:
        logger.debug('Network timeout: {}'.format(url))
        return None
    else:
        return r


def getThumbnail(url, recordIdentifier, rowInDB):
    '''Puts the thumbnail file in its place on the image server, and returns its URL.'''

    r = makeThumbnailRequest(requests.get, url, True, True)
    if r is None:
        # disaster has struck
        raise Exception('Thumbnail was available, and now it\'s not: {}'.format(url))

    basename = url.split('/')[-1]
    extension = os.path.splitext(basename)[1]
    if extension == '':
        extension = guess_extension(r.headers['content-type'])
        if extension is None:
            # throw error
            logger.error('Cannot determine file type for {}'.format(url))
            pass

    # used as the local filename too
    # need to save the file locally with slashes escaped
    s3Key = urllib.parse.quote(recordIdentifier, safe='')

    # url to the thumbnail needs to be encoded twice
    s3KeyDoublyEncoded = urllib.parse.quote(s3Key, safe='')

    # should use the same name for S3 object
    filepath = os.path.join(
        os.path.abspath(os.path.expanduser(config['S3']['thumbnail_dir'])),
        rowInDB['institution_key'],
        rowInDB['collection_key'],
        s3Key + extension
        )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # https://stackoverflow.com/a/16696317
    with open(filepath, 'wb') as f:
        logger.info('Saving thumbnail to "{}"'.format(filepath))
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    logger.debug('Thumbnail written to {}'.format(filepath))

    # upload to S3
    s3.put_object(Bucket=config['S3']['bucket'], Key=s3Key, Body=open(filepath, 'rb'), ContentType=guess_type(url)[0])

    # return URL of image
    thumbnailUrl = urllib.parse.urlunparse(('http', config['S3']['bucket'], s3KeyDoublyEncoded, '', '', ''))
    logger.debug('Thumbnail available at {}'.format(thumbnailUrl))
    return thumbnailUrl


def deleteThumbnail(s3Key):
    s3.delete_object(Bucket=config['S3']['bucket'], Key=s3Key)


def main():

    logger.info('--- STARTING RUN ---')
    logger.info('')

    solrUrl = config['Solr']['url']
    tinydbPath = os.path.abspath(os.path.expanduser(config['TinyDB']['path']))

    # make sure URL is well-formed
    if not validators.url(solrUrl):
        logger.critical('{} is not a valid URL'.format(solrUrl))
        exit(1)
    else:
        solr = pysolr.Solr(solrUrl)

    # make sure database exists
    try:
        with open(tinydbPath, 'r') as f:
            pass
        db = TinyDB(tinydbPath)

    except:
        logger.critical('{} does not exist'.format(tinydbPath))
        exit(1)

    for row in db:

        Row = Query()

        if row['new'] is True:
            mode = '--baseline'
        else:
            mode = '--incremental'

        command = ['resync', mode, '--noauth', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], '--changelist-uri', row['changelist_uri'], row['url_map_from'], os.path.join(row['file_path_map_to'], row['institution_key'], row['collection_key'])]

        try:
            actions = subprocess.check_output(
                command,
                stderr=subprocess.STDOUT
                )
            if row['new'] == True:
                db.update({'new': False}, Row['institution_key'] == row['institution_key'] and Row['collection_key'] == row['collection_key'])
        except subprocess.CalledProcessError as e:
            logger.error('Invalid invocation of "resync" with collection {}: {}'.format(row['collection_key'], e))
            # TODO: note that we should come back to this collection later
            continue

        for line in actions.splitlines():

            '''
            # TODO: fault tolerance
            failures = row['_failures'] or []
            '''

            action = line.split(b' ')[0]
            if action in [b'created:', b'updated:', b'deleted:']:

                localFile = line.split(b' -> ')[1]

                with open(localFile) as fp:
                    soup = BeautifulSoup(fp, 'xml')

                    try:
                        # if deleted, skip to next record
                        if soup.find('header')['status'] == 'deleted':
                            continue
                    except KeyError:
                        logger.debug('Cenerating Solr document from records in "{}"'.format(localFile))
                        recordIdentifier = soup.find('identifier').string
                        tags = soup.find('dc').contents

                oaiPmhHost = urllib.parse.urlparse(row['url_map_from']).netloc

                if action == b'created:':

                    logger.info('Creating Solr document for {}'.format(recordIdentifier))

                    thumbnailUrl = findThumbnailUrl(soup, bsFilters)
                    if thumbnailUrl is not None:
                        logger.debug('Found thumbnail URL: {}'.format(thumbnailUrl))
                        thumbnailUrl = getThumbnail(thumbnailUrl, recordIdentifier, row)
                        logger.debug('Got thumbnail')

                    doc = createSolrDoc(recordIdentifier, row, thumbnailUrl, tags, oaiPmhHost)
                    logger.debug('Created Solr doc: {}'.format(dumps(doc, indent=4)))
                    try:
                        solr.add([doc])
                        logger.debug('Submitted Solr doc!')
                    except:
                        logger.error('Something went wrong while trying to send data to Solr')
                        '''
                        # TODO: fault tolerance
                        failures.push({
                            'collection_key': recordIdentifier,
                            'action': action
                            })
                        '''
                        continue

                elif action == b'updated:':

                    logger.info('Updating Solr document for {}'.format(recordIdentifier))

                    thumbnailUrl = findThumbnailUrl(soup, bsFilters)
                    if thumbnailUrl is not None:
                        logger.debug('Found thumbnail URL: {}'.format(thumbnailUrl))
                        thumbnailUrl = getThumbnail(thumbnailUrl, recordIdentifier, row)
                        logger.debug('Got thumbnail')

                    doc = createSolrDoc(recordIdentifier, row, thumbnailUrl, tags, oaiPmhHost)
                    logger.debug('Created Solr doc: {}'.format(dumps(doc, indent=4)))
                    try:
                        solr.add([doc])
                        logger.debug('Submitted Solr doc!')
                    except:
                        logger.error('Something went wrong while trying to send data to Solr')
                        '''
                        # TODO: fault tolerance
                        failures.push({
                            'collection_key': recordIdentifier,
                            'action': action
                            })
                        '''
                        continue

                elif action == b'deleted:':

                    # TODO: delete the associated thumbnail as well
                    logger.info('Deleting Solr document for {}'.format(recordIdentifier))
                    deleteThumbnail(recordIdentifier)

                    try:
                        solr.delete(id=recordIdentifier)
                    except:
                        logger.error('Something went wrong while trying to send data to Solr')
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

    logger.info('')
    logger.info('---  ENDING RUN  ---\n')

if __name__ == '__main__':
    main()
