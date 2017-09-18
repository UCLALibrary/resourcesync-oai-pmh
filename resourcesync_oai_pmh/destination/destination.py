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
import pysolr
import re
from requests import get
import subprocess
import sys
from tinydb import TinyDB, Query
import urllib.parse
import validators

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

# list of BeautifulSoup filters to pass to the find_all function
bsFilters = [
    'identifier',
    'identifier.thumbnail',
    re.compile('identifier.*')
    ]

# regular expressions used for matching non-standard date formats
regexes = {
    'match': {},
    'substitution': {}
    }
regexes['match']['mm'] = r'(?:0[1-9]|1[0-2])'
regexes['match']['dd'] = r'(?:0[1-9]|[1-2]\d|3[1-2])'
regexes['match']['mon'] = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
regexes['match']['time'] = r'\d{1,2}[.:]\d{2}(?:[ap]\.?m)?'
regexes['match']['year'] = r'[1-9]\d*'
regexes['match']['year?'] = r'[1-9]\d*[-*?]?'
regexes['match']['year-mm'] = r'{}(?:(?:[-/]{})|[-*?])?(?=\D|$)'.format(
    regexes['match']['year'],
    regexes['match']['mm'])

regexes['match']['year-year'] = r'{}\s*[-/]\s*{}'.format(
    regexes['match']['year-mm'],
    regexes['match']['year-mm'])

regexes['match']['dd-mon-year-time'] = r'{}\s+{}\s+{}(?:\.\s+{})?'.format(
    regexes['match']['dd'],
    regexes['match']['mon'],
    regexes['match']['year'],
    regexes['match']['time'])

regexes['match']['year/m'] = r'{}/\d'.format(regexes['match']['year'])
regexes['match']['century'] = r'(?:1st|2nd|3rd|(?:[4-9]|1[0-9]|20)th)\s+[cC](?:entury)?'

# order of alternate patterns is important
regexes['match']['alternator'] = r'(?:({})|({})|({})|({})|({}))'.format(
    regexes['match']['century'],
    regexes['match']['year-year'],
    regexes['match']['dd-mon-year-time'],
    regexes['match']['year/m'],
    regexes['match']['year?'])

regexes['match']['suffix-bce'] = r'BC|B\.C\.|BCE|B\.C\.E\.'
regexes['match']['suffix-ce']= r'AD|A\.D\.|CE|C\.E\.'
regexes['match']['suffix'] = r'(?:{}|{})'.format(
    regexes['match']['suffix-bce'],
    regexes['match']['suffix-ce'])

regexes['match']['date'] = r'{}(?:\s+({}))?'.format(
    regexes['match']['alternator'],
    regexes['match']['suffix'])

regexes['substitution']['year-year'] = r'({})\s*[-/]\s*({})'.format(
    regexes['match']['year-mm'],
    regexes['match']['year-mm'])

regexes['substitution']['dd-mon-year-time'] = r'{}\s+{}\s+({})(?:\.\s+{})?'.format(
    regexes['match']['dd'],
    regexes['match']['mon'],
    regexes['match']['year'],
    regexes['match']['time'])

def addValuePossiblyDuplicateKey(key, value, dic):
    '''Adds a key-value pair to a dictionary that may already have a value for that key. If that's the case, put both values into a list. This is how pysolr wants us to represent duplicate fields.'''

    if key in dic:
        if isinstance(dic[key], collections.MutableSequence):
            dic[key].append(value)
        else:
            dic[key] = [dic[key], value]
    else:
        dic[key] = value


def createSolrDoc(identifier, rowInDB, thumbnailurl, tags):
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

    for tag in tags:

        # ignore newlines and other whitespace in the list of tags
        if tag.name is None:
            continue

        # only process Dublin Core fields (no qualified DC)
        try:
            name = tagNameToColumn[tag.name]
        except KeyError as e:
            continue
        else:
            value = tag.string
            addValuePossiblyDuplicateKey(name, value, doc)

            # build up a set of all the years included in the metadata
            if name == tagNameToColumn['date']:
                years = years | cleanAndNormalizeDate(value)

    decades = facet_decades(years)
    for decade in decades:
        addValuePossiblyDuplicateKey('decade', decade, doc)

    logger.debug('years "{}" -> decades "{}"'.format(years, decades))

    return doc


def facet_decades(years):
    '''Returns a set of decades that spans all of the years in the input set.'''

    currentYear = date.today().year
    matches = set(filter(lambda a: a <= currentYear, years))
    if len(matches) == 0:
        return set()

    # round min down to nearest decade
    start = min(matches) // 10 * 10
    # add 1 just in case max ends with 0
    end = max(matches) + 1
    return set(range(start, end, 10))


def resolveUnknownOnes(i):
    '''i is a string that represents a year with a possibly missing ones value, like "199-?" or "199?". Round down to the nearest decade'''

    m = re.compile('(\d{4})').match(i)
    if m is not None:
        return int(m.group(1))
    else:
        m = re.compile('(\d{1,3})[-*?]').match(i)
        return i if m is None else int(m.group(1) + '0')


def dateMatchToInt(m):
    '''Maps a match of regexes['match']['date'] to a set of years.'''

    years = set()
    '''
    Match indices:
    0 -> 'century'
    1 -> 'year-year'
    2 -> 'dd-mon-year-time'
    3 -> 'year/m'
    4 -> 'year?'
    5 -> 'suffix'
    '''
    try:
        if m[0] != '':
            century = int(re.match(re.compile('\d+'), m[0]).group(0))
            years = {100 * (century - 1), 100 * (century - 1) + 99}
        elif m[1] != '':
            years = {int(resolveUnknownOnes(y.strip())) for y in re.sub(regexes['substitution']['year-year'], r'\1>|<\2', m[1]).split('>|<')}
        elif m[2] != '':
            years = {int(resolveUnknownOnes(re.sub(regexes['substitution']['dd-mon-year-time'], r'\1', m[2]).strip()))}
        elif m[3] != '':
            # TODO: remove this case, we aren't handling it
            a = m[3].split('/')
            prefix = a[0][:-1]
            years = reduce(lambda x, y: x | {int('{}{}'.format(prefix, y))}, a[1:], {int(a[0])})
        elif m[4] != '':
            years = {int(resolveUnknownOnes(m[4]))}
        else:
            # error
            raise Error
    except ValueError as e:
        logger.error('An error occurred while trying to match "{}": {}'.format(m, e))

    if m[5] != '' and re.compile(regexes['match']['suffix-bce']).match(m[5]) is not None:
        # move everything to the left side of year 0 on the timeline
        if m[0] != '':
            years = {100 * -century, 100 * -century + 99}
        else:
            years = {-x for x in years}

    logger.debug('Mapping match to years: {} -> {}'.format(m, years))
    return years


def cleanAndNormalizeDate(dateString):
    '''Returns a normalized set of years (integers) found in the input string.'''

    # first see if dateutil can parse the date string
    try:
        return {parse(dateString).year}

    except ValueError:
        try:
            # strip alphabetical chars and spaces from the left side and try again
            return {parse(dateString.lstrip('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ')).year}
        except ValueError:
            # find as many substrings that look like years as possible
            matches = re.findall(re.compile(regexes['match']['date']), dateString)
            logger.debug('{} date string matches found in "{}"'.format(len(matches), dateString))
            if len(matches) > 0:
                return reduce(lambda x, y: x | y, [dateMatchToInt(m) for m in matches], set())
            else:
                return set()


def findThumbnailUrl(bs, filters):
    '''Return the URL of the thumbnail for a Dublin Core record. If none exists, return None.

    bs - BeautifulSoup representation of the metadata file
    filters - a list of filters to pass to the find_all function, that denote where a URL might live
    '''

    for f in filters:

        # search for tags that match the filter (can be regex or string, see )
        tags = bs.find_all(f)

        for tag in tags:
            possibleUrl = tag.string
            if validators.url(possibleUrl):
                # TODO: maybe check path extension before doing get request?
                r = get(possibleUrl)
                # TODO: get image types from config
                m = re.search(re.compile('image/(?:jpeg|tiff|png)'), r.headers['content-type'])
                if m is not None:
                    return possibleUrl
    return None


def getThumbnail(url, recordIdentifier):
    '''Puts the thumbnail file in its place on the image server, and returns its URL.'''

    r = get(url, stream=True)

    basename = url.split('/')[-1]
    extension = os.path.splitext(basename)[1]
    if extension == '':
        extension = guess_extension(r.headers['content-type'])
        if extension is None:
            # throw error
            pass

    # need to save the file locally with slashes escaped, and should use the same name for S3 object
    s3Key = urllib.parse.quote(recordIdentifier, safe='') + extension

    # url to the thumbnail needs to be encoded twice
    s3KeyDoublyEncoded = urllib.parse.quote(urllib.parse.quote(recordIdentifier, safe='')) + extension

    filepath = os.path.join(
        os.path.abspath(os.path.expanduser(config['S3']['thumbnail_dir'])),
        s3Key
        )

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
    u = urllib.parse.urlunparse(('http', config['S3']['bucket'], s3KeyDoublyEncoded, '', '', ''))
    logger.debug('Thumbnail available at {}'.format(u))
    return u


def main():

    logger.info('--- STARTING RUN ---')
    logger.info('')

    solrUrl = config['Solr']['url']
    tinydbPath = os.path.abspath(os.path.expanduser(config['TinyDB']['path']))

    # make sure URL is well-formed
    if not validators.url(solrUrl):
        logger.critical('{} is not a valid URL'.format(solrUrl))
        exit(1)

    try:
        # throws exception if the file doesn't exist
        with open(tinydbPath, 'r') as f:
            pass
    except:
        logger.critical('{} does not exist'.format(tinydbPath))
        exit(1)

    solr = pysolr.Solr(solrUrl)
    db = TinyDB(tinydbPath)

    for row in db:

        Row = Query()

        if row['new'] == True:

            # baseline sync
            command = ['resync', '--baseline', '--noauth', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], row['url_map_from'], row['file_path_map_to']]
            try:
                actions = subprocess.check_output(
                    command,
                    stderr=subprocess.STDOUT
                    )
            except subprocess.CalledProcessError as e:
                logger.error('Invalid invocation of "resync" with collection {}'.format(row['collection_key']))
                logger.error(e)
                continue

            # set row.new = False
            db.update({'new': False}, Row['collection_key'] == row['collection_key'])

        else:

            # incremental sync
            command = ['resync', '--incremental', '--noauth', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], '--changelist-uri', row['changelist_uri'], row['url_map_from'], row['file_path_map_to']]
            try:
                actions = subprocess.check_output(
                    command,
                    stderr=subprocess.STDOUT
                    )
            except subprocess.CalledProcessError as e:
                logger.error('Invalid invocation of "resync" with collection {}'.format(row['collection_key']))
                logger.error(e)
                continue

        for line in actions.splitlines():

            '''
            # TODO: fault tolerance
            failures = row['_failures'] or []
            '''

            action = line.split(b' ')[0]
            if action in [b'created:', b'updated:', b'deleted:']:

                localFile = line.split(b' -> ')[1]
                logger.debug('Cenerating Solr document from records in "{}"'.format(localFile))

                with open(localFile) as fp:
                    soup = BeautifulSoup(fp, 'xml')
                    recordIdentifier = soup.find('identifier').string
                    tags = soup.find('dc').contents

                if action == b'created:':

                    logger.info('Creating Solr document for {}'.format(recordIdentifier))

                    thumbnailUrl = findThumbnailUrl(soup, bsFilters)
                    if thumbnailUrl is not None:
                        thumbnailUrl = getThumbnail(thumbnailUrl, recordIdentifier)

                    doc = createSolrDoc(recordIdentifier, row, thumbnailUrl, tags)
                    logger.debug('Sending to Solr: {}'.format(dumps(doc, indent=4)))
                    try:
                        solr.add([doc])
                        logger.debug('Thumbnail: {}'.format(thumbnailUrl))
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
                        thumbnailUrl = getThumbnail(thumbnailUrl, recordIdentifier)

                    doc = createSolrDoc(recordIdentifier, row, thumbnailUrl, tags)
                    logger.debug('Sending to Solr: {}'.format(dumps(doc, indent=4)))
                    try:
                        solr.add([doc])
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
