#!/usr/bin/python3

# rs_oaipmh_dest.py
#
# This script is to be run on the ResourceSync destination server.
# It does two main things:
# - Synchronize our local copy of each collection with updates from member institutions.
# - Propagate these changes to Solr

import os
import pdb
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
from requests import get
from json import dumps
from functools import reduce

noThumbnailExistsUrl = 'http://nothumb.com'

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

bsFilters = [
    'identifier',
    'identifier.thumbnail',
    re.compile('identifier.*')
    ]

### regular expressions ###

# 1. used for cleanAndNormalizeDate
twoDigitMonth = r'(?:0[1-9]|1[0-2])'
twoDigitDay = r'(?:0[1-9]|[1-2]\d|3[1-2])'
threeCharMonth = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
timePattern = r'\d{1,2}[.:]\d{2}(?:[ap]\.?m)?'
yearPattern = r'[1-9]\d*'
yearPatternMissingOnes = r'[1-9]\d*[-*?]?'
yearMonthPattern = r'{}(?:(?:[-/]{})|[-*?])?(?=\D|$)'.format(yearPattern, twoDigitMonth)
yearRangePattern = r'{}\s*[-/]\s*{}'.format(yearMonthPattern, yearMonthPattern)
dmyt = r'{}\s+{}\s+{}(?:\.\s+{})?'.format(twoDigitDay, threeCharMonth, yearPattern, timePattern)
consecutiveYearsPattern = r'{}/\d'.format(yearPattern)
centuryPattern = r'(?:1st|2nd|3rd|(?:[4-9]|1[0-9]|20)th)\s+[cC](?:entury)?'

# the order of checks is important (try to match yeraRangePattern before yearPatternMissingOnes
datePattern = r'(?:({})|({})|({})|({})|({}))'.format(centuryPattern, yearRangePattern, dmyt, consecutiveYearsPattern, yearPatternMissingOnes)
suffixBeforeYear0 = r'BC|B\.C\.|BCE|B\.C\.E\.'
suffixAfterYear0 = r'AD|A\.D\.|CE|C\.E\.'
suffixPattern = r'(?:{}|{})'.format(suffixBeforeYear0, suffixAfterYear0)
finalPattern = r'{}(?:\s+({}))?'.format(datePattern, suffixPattern)

# 2. used for dateMatchToInt
yearRangeCaptureYearsPattern = r'({})\s*[-/]\s*({})'.format(yearMonthPattern, yearMonthPattern)
dmytCaptureYearPattern = r'{}\s+{}\s+({})(?:\.\s+{})?'.format(twoDigitDay, threeCharMonth, yearPattern, timePattern)

def addValuePossiblyDuplicateKey(key, value, dic):
    '''Adds a key-value pair to a dictionary that may already have a value for that key. If that's the case, put both values into a list. This is how pysolr wants us to represent duplicate fields.'''

    if key in dic:
        if isinstance(dic[key], collections.MutableSequence):
            dic[key].append(value)
        else:
            dic[key] = [dic[key], value]
    else:
        dic[key] = value

def createSolrDoc(identifier, colname, instname, thumbnailurl, tags):
    '''Maps a Dublin Core record to a Solr document to be indexed.'''

    doc = {
        'id': identifier,
        #'thumbnail_url': thumbnailurl,
        'collectionName': colname,
        'institutionName': instname
    }
    dates = set()

    for tag in tags:

        # ignore newlines and other whitespace
        if tag.name is None:
            continue

        try:
            name = tagNameToColumn[tag.name]
        except KeyError as e:
            # only process Dublin Core fields
            continue

        value = tag.string

        addValuePossiblyDuplicateKey(name, value, doc)

        if tag.name == 'date':
            dates = dates | cleanAndNormalizeDate(value)

    decades = facet_decades(dates)

    logging.debug('Dates: {}'.format(dates))
    logging.debug('Decades: {}'.format(decades))

    for decade in decades:
        addValuePossiblyDuplicateKey('decade', decade, doc)

    logging.debug(dumps(doc, indent=4))
    return doc

def facet_decades(years):
    '''Returns a set of decades that spans all of the years in "years".'''

    currentYear = date.today().year

    # matches = set(filter(lambda a: a >= 1000, years))
    matches = set(filter(lambda a: a <= currentYear, years))
    if len(matches) == 0:
        return set()

    start = min(matches) // 10 * 10
    end = max(matches) + 1
    return set(range(start, end, 10))

def resolveUnknownOnes(i):
    '''i is a string that represents a year with a possibly missing ones value.'''
    m = re.compile('(\d{4})').match(i)
    if m is not None:
        return int(m.group(1))
    else:
        m = re.compile('(\d{1,3})[-*?]').match(i)
        return i if m is None else int(m.group(1) + '0')

def dateMatchToInt(m):
    try:
        if m[0] != '':
            # century
            century = int(re.match(re.compile('\d+'), m[0]).group(0))
            years = {100 * (century - 1), 100 * (century - 1) + 99}
        elif m[1] != '':
            # year range
            years = {int(resolveUnknownOnes(y.strip())) for y in re.sub(yearRangeCaptureYearsPattern, r'\1>|<\2', m[1]).split('>|<')}
        elif m[2] != '':
            # dmyt
            years = {int(resolveUnknownOnes(re.sub(dmytCaptureYearPattern, r'\1', m[2]).strip()))}
        elif m[3] != '':
            # consecutive years
            # TODO: remove
            a = m[3].split('/')
            prefix = a[0][:-1]
            years = reduce(lambda x, y: x | {int('{}{}'.format(prefix, y))}, a[1:], {int(a[0])})
        elif m[4] != '':
            # year
            years = {int(resolveUnknownOnes(m[4]))}
        else:
            # error
            raise Error
    except ValueError as e:
        logging.error(e)


    if m[5] != '' and re.compile(suffixBeforeYear0).match(m[5]) is not None:
        # convert years to negatives
        # if century notation, go further negative
        if m[0] != '':
            years = {100 * -century, 100 * -century + 99}
        else:
            years = {-x for x in years}

    logging.debug('Mapping match to years: {} -> {}'.format(m, years))
    return years

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

    except ValueError:
        # If not, find as many substrings that look like years as possible
        try:
            # strip alphabetical chars and spaces from the left side and try again
            return {parse(dateString.lstrip('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ')).year}
        except ValueError:
            matches = re.findall(re.compile(finalPattern), dateString)
            logging.debug('{} date string matches found in "{}"'.format(len(matches), dateString))
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

        # search for tags that match the filter (can be regex or string, see https://www.crummy.com/software/BeautifulSoup/bs4/doc/#find-all)
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

def getThumbnail(url):
    '''Puts the thumbnail file in its place on the image server, and returns its URL.'''
    # TODO: Put thumbnail file somewhere to be served up
    # https://stackoverflow.com/a/16696317
    thumbpath = '~/thumbnails/'
    filename = thumbpath + url.split('/')[-1]
    r = get(url, stream=True)
    with open(os.path.expanduser(filename), 'wb') as f:
        logging.info('Saving thumbnail to "{}"'.format(filename))
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    # TODO: upload to S3?
    # use boto

    return 'http://example.com/{}'.format(filename)

def main():

    logging.basicConfig(
        level=logging.DEBUG,
        filename='rs_oaipmh_dest.log',
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
            command = ['resync', '--baseline', '--noauth', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], row['url_map_from'], row['file_path_map_to']]
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
            command = ['resync', '--incremental', '--noauth', '--verbose', '--logger', '--delete', '--sitemap', row['resourcelist_uri'], '--changelist-uri', row['changelist_uri'], row['url_map_from'], row['file_path_map_to']]
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

                    thumbnailUrl = findThumbnailUrl(soup, bsFilters)
                    if thumbnailUrl is not None:
                        thumbnailUrl = getThumbnail(thumbnailUrl)
                    else:
                        thumbnailUrl = noThumbnailExistsUrl

                    doc = createSolrDoc(recordIdentifier, row['collection_key'], row['institution_key'], thumbnailUrl, tags)
                    try:
                        solr.add([doc])
                        logging.debug('Thumbnail: {}'.format(thumbnailUrl))
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

                    thumbnailUrl = findThumbnailUrl(soup, bsFilters)
                    if thumbnailUrl is not None:
                        thumbnailUrl = getThumbnail(thumbnailUrl)
                    else:
                        thumbnailUrl = noThumbnailExistsUrl

                    doc = createSolrDoc(recordIdentifier, row['collection_key'], row['institution_key'], thumbnailUrl, tags)
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

                    # TODO: delete the associated thumbnail as well
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
