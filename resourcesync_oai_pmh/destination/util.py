#!/usr/bin/python3

from bs4 import BeautifulSoup
import collections
import csv
from datetime import date
from dateutil.parser import parse
import functools
from functools import reduce
from json import dumps
import logging
import logging.config
import os
import re
from requests import get
from sickle import Sickle
import sys
from tinydb import TinyDB, Query
import urllib.parse
import pdb


class DateCleanerAndFaceter:
    '''
    Class for cleaning and creating decade or year facets for dates.
    '''

    def __init__(self, data):
        '''
        Initialize the object for use.

        data - can be a single string or a set of strings
        '''

        self.data = data

        # regular expressions used for matching non-standard date formats
        # TODO: move to separate file
        self.regexes = {
            'match': {},
            'substitution': {},
            'capture': {}
            }

        # years before 0
        self.regexes['match']['suffix-bce'] = r'BC|B\.C\.|BCE|B\.C\.E\.'

        # years after 0
        self.regexes['match']['suffix-ce']= r'AD|A\.D\.|CE|C\.E\.'

        # a suffix may indicate years before 0 or years after 0
        self.regexes['match']['suffix'] = r'(?:{}|{})'.format(
            self.regexes['match']['suffix-bce'],
            self.regexes['match']['suffix-ce'])

        # two-digit representation of a month: 01 - 12
        self.regexes['match']['mm'] = r'(?:0[1-9]|1[0-2])'

        # two-digit representation of a day of a month: 01 - 31
        self.regexes['match']['dd'] = r'(?:0[1-9]|[1-2]\d|3[0-1])'

        # three-character representation of a month
        self.regexes['match']['mon'] = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'

        # time: e.g., 02:00 am, 12:55 P.M., 3:40
        self.regexes['match']['time'] = r'\d{1,2}[.:]\d{2}(?:[apAP]\.?[mM]\.?)?'

        # require a 1 or 2 digit year to have a suffix
        self.regexes['match']['year0,1'] = r'[1-9]\d{{0,1}}'
        self.regexes['match']['year0,1-plus-suffix'] = r'{} {}'.format(
            self.regexes['match']['year0,1'],
            self.regexes['match']['suffix'])

        # 3 or 4 digit years may or may not have a suffix
        self.regexes['match']['year2,3'] = r'[1-9]\d{{2,3}}'.format(self.regexes['match']['suffix'])
        self.regexes['match']['year2,3-plus-suffix'] = r'{}(?: {})?'.format(
            self.regexes['match']['year2,3'],
            self.regexes['match']['suffix'])

        # a year can have 1, 2, 3, or 4 digits, and may or may not have a suffix according to the above
        self.regexes['match']['year'] = r'(?:{}|{})'.format(
            self.regexes['match']['year0,1-plus-suffix'],
            self.regexes['match']['year2,3-plus-suffix'])

        #
        # year ranges
        #

        # parsing them is complicated, so we'll have a subset of special rules for them
        # we want to capture certain aspects of the year range
        self.regexes['capture']['year0,1-plus-suffix'] = r'({}) ({})'.format(
            self.regexes['match']['year0,1'],
            self.regexes['match']['suffix'])

        # 3 or 4 digit years may or may not have a suffix
        self.regexes['capture']['year2,3-plus-suffix'] = r'({})(?: ({}))?'.format(
            self.regexes['match']['year2,3'],
            self.regexes['match']['suffix'])

        # a year can have 1, 2, 3, or 4 digits, and may or may not have a suffix according to the above
        # 1: 1-2 digit year
        # 2: suffix
        # 3: 3-4 digit year
        # 4: suffix
        self.regexes['capture']['year'] = r'(?:{}|{})'.format(
            self.regexes['capture']['year0,1-plus-suffix'],
            self.regexes['capture']['year2,3-plus-suffix'])

        # sometimes metadata indicates uncertainty about a year, either with a question mark at the end or some other character in place of the one's digit
        self.regexes['match']['year?'] = r'[1-9]\d{1,2}\d?[-*?]'

        # matches a year followed by a 2 digit month (must not be followed by another digit), the year can have a mystery one's place
        # assume that if a month is given, there's no suffix
        self.regexes['match']['year-mm'] = r'{}(?:(?:[-/]{})|[-*?])?(?=\D|$)'.format(
            self.regexes['match']['year'],
            self.regexes['match']['mm'])

        # matches a range of years, separated by either - or /
        self.regexes['match']['year-year'] = r'{}\s*[-/]\s*{}'.format(
            self.regexes['match']['year-mm'],
            self.regexes['match']['year-mm'])

        self.regexes['match']['dd-mon-year-time'] = r'{}\s+{}\s+{}(?:\.\s+{})?'.format(
            self.regexes['match']['dd'],
            self.regexes['match']['mon'],
            self.regexes['match']['year'],
            self.regexes['match']['time'])

        # matches a century string
        self.regexes['match']['century'] = r'(?:1st|2nd|3rd|(?:[4-9]|1[0-9]|20)th)\s+[cC](?:entury)?'
        self.regexes['match']['century-plus-suffix'] = r'{}(?:\s+{})?'.format(
            self.regexes['match']['century'],
            self.regexes['match']['suffix'])

        # order of alternate patterns is important
        self.regexes['match']['date'] = r'(?:({})|({})|({})|({})|({}))'.format(
            self.regexes['match']['century-plus-suffix'],
            self.regexes['match']['year-year'],
            self.regexes['match']['dd-mon-year-time'],
            self.regexes['match']['year?'],
            self.regexes['match']['year'])

        # split the year range in half
        self.regexes['substitution']['year-year-splitter'] = r'({})\s*[-/]\s*({})'.format(
            self.regexes['match']['year-mm'],
            self.regexes['match']['year-mm'])

        self.regexes['substitution']['dd-mon-year-time'] = r'{}\s+{}\s+({})(?:\.\s+{})?'.format(
            self.regexes['match']['dd'],
            self.regexes['match']['mon'],
            self.regexes['match']['year'],
            self.regexes['match']['time'])

        # capture century info
        self.regexes['capture']['century-plus-suffix'] = r'({})(?:\s+({}))?'.format(
            self.regexes['match']['century'],
            self.regexes['match']['suffix'])


    # Public methods


    def decades(self, disjoint=True):
        '''
        Returns a set of decades that covers all of the years and year ranges in the data.

        disjoint - whether or not tp exclude decades in the interim between the earliest and latest decades
        '''

        try:
            return self.decadeSet

        except AttributeError:
            # set self.decadeSet and return it
            self.decadeSet = set()
            try:
                # multi valued
                assert isinstance(self.data, set)
                for datum in self.data:
                    preprocessedYearData = self.__extractYearData(datum)
                    self.decadeSet = self.decadeSet | self.__enumerateDecades(preprocessedYearData, disjoint)
            except (AssertionError, TypeError):
                # single value
                preprocessedYearData = self.__extractYearData(self.data)
                self.decadeSet = self.decadeSet | self.__enumerateDecades(preprocessedYearData, disjoint)

            return self.decadeSet


    def years(self, disjoint=True):
        '''
        Returns a set of years that covers all of the years and year ranges in the data.

        disjoint - whether or not to exclude years in the interim between the earliest and latest years
        '''

        try:
            return self.yearSet

        except AttributeError:
            # set self.yearSet and return it
            self.yearSet= set()
            try:
                # multi valued
                assert isinstance(self.data, set)
                for datum in self.data:
                    preprocessedYearData = self.__extractYearData(datum)
                    self.yearSet= self.yearSet | self.__enumerateYears(preprocessedYearData, disjoint)

            except TypeError:
                # single value
                preprocessedYearData = self.__extractYearData(self.data)
                self.yearSet = self.__enumerateYears(preprocessedYearData, disjoint)

            return self.yearSet


    # Private methods


    def __dateMatchToIntOrTuple(self, m):
        '''
        Maps a match of regexes['match']['date'] to a tuple of years, or a single year.

        m - the re.match object

        Match indices:
            0 -> 'century'
            1 -> 'year-year'
            2 -> 'dd-mon-year-time'
            3 -> 'year?'
            4 -> 'year'
        '''

        years = set()
        try:
            if m[0] != '':
                # year-range derived from a century
                century = int(re.match(re.compile('\d+'), m[0]).group(0))

                match = re.compile(self.regexes['capture']['century-plus-suffix']).match(m[0])
                suffix = match.group(2)
                if suffix:
                    if re.compile(self.regexes['match']['suffix-bce']).match(suffix) is not None:
                        years = (100 * -century, 100 * -century + 99)
                    else:
                        years = (100 * (century - 1), 100 * (century - 1) + 99)
                else:
                    years = (100 * (century - 1), 100 * (century - 1) + 99)
            elif m[1] != '':
                # explicit year-range

                # FIXME: spaghetti code, but it works!
                rangeOfStuff = []
                i = 0
                firstNone = None
                for y in re.sub(self.regexes['substitution']['year-year-splitter'], r'\1>|<\2', m[1]).split('>|<'):
                    # get rid of whitespace
                    y = y.strip()
                    match = re.compile(self.regexes['capture']['year']).match(y)

                    # if there is a suffix, one of these will not be None
                    suffix = match.group(2) or match.group(4)
                    if suffix:
                        if i == 0:
                            firstNone = False

                        if re.compile(self.regexes['match']['suffix-bce']).match(suffix) is not None:
                            rangeOfStuff.append(-1 * int(match.group(1) or match.group(3)))
                        else:
                            rangeOfStuff.append(int(match.group(1) or match.group(3)))
                    else:
                        if i == 0:
                            firstNone = True
                        rangeOfStuff.append(int(match.group(1) or match.group(3)))

                    i += 1
                if firstNone:
                    if rangeOfStuff[1] <= 0:
                        rangeOfStuff[0] = -1 * rangeOfStuff[0]

                years = (rangeOfStuff[0], rangeOfStuff[1])
            elif m[2] != '':
                # extract single year
                prep = re.sub(self.regexes['substitution']['dd-mon-year-time'], r'\1', m[2]).strip()
                years = int(prep)
            elif m[3] != '':
                # year with unknown ones
                y = m[3].strip()
                match = re.compile(r'[1-9]\d{3}').match(y)
                if match is None:
                    years = int(self.__resolveUnknownOnes(y))

                else:
                    years = int(match.group(0))

            elif m[4] != '':
                # plain old year
                match = re.compile(self.regexes['capture']['year']).match(m[4])
                suffix = match.group(2) or match.group(4)
                if suffix:
                    if re.compile(self.regexes['match']['suffix-bce']).match(suffix) is not None:
                        years = -1 * int(match.group(1) or match.group(3))
                    else:
                        years = int(match.group(1) or match.group(3))
                else:
                    years = int(match.group(1) or match.group(3))
            else:
                raise Error

        except ValueError as e:
            #logger.error('An error occurred while trying to match "{}": {}'.format(m, e))
            pass

        '''
        if m[5] != '' and re.compile(self.regexes['match']['suffix-bce']).match(m[5]) is not None:
            # move everything to the left side of year 0 on the timeline
            if m[0] != '':
                years = (100 * -century, 100 * -century + 99)
            elif m[1] != '':
                years = (-years[0], -years[1])
            else:
                years = -years
        '''

        #logger.debug('Mapping match to years: {} -> {}'.format(m, years))
        return years


    def __enumerateDecades(self, preprocessedData, disjoint):
        '''
        Return a set of decades. If disjoint is false, returns a set of decades that spans the entire range.

        preprocessedData - a heterogeneous set of ints and tuples of ints
        '''

        if disjoint:
            decades = set()
            for decade in preprocessedData:
                if isinstance(decade, int):
                    decades.add(decade // 10 * 10)
                elif isinstance(decade, tuple):
                    decades = decades | set(range(decade[0] // 10 * 10, decade[1] + 1, 10))

            return decades


    def __enumerateYears(self, preprocessedData, disjoint):
        '''
        Return a set of years. If disjoint is false, returns a set of years that spans the entire range.

        preprocessedData - a heterogeneous set of ints and tuples of ints
        '''
        pass


    def __extractYearData(self, dateString):
        '''
        Extracts the year(s) and/or year range(s) embedded in the dateString, and return a set of ints (year) and/or tuples of ints (year range, start/end).

        dateString - the string containing the dirty date
        '''

        try:
            # first see if dateutil can parse the date string
            # simplest case, a single year
            return {parse(dateString).year}

        except ValueError:
            try:
                # strip alphabetical chars and spaces from the left side and try again
                alpha = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
                return {parse(dateString.lstrip(alpha + ' ')).year}

            except ValueError:
                # find as many substrings that look like dates as possible
                matches = re.findall(re.compile(self.regexes['match']['date']), dateString)
                #logger.debug('{} date string matches found in "{}"'.format(len(matches), dateString))
                if len(matches) > 0:
                    return {self.__dateMatchToIntOrTuple(m) for m in matches}
                else:
                    return set()


    def __resolveUnknownOnes(self, i):
        '''
        i is a string that represents a year with a possibly missing ones value, like "199-?" or "199?". Round down to the nearest decade.
        '''

        m = re.compile('(^\d{4}$)').match(i)
        if m is not None:
            return int(m.group(1))
        else:
            m = re.compile('(^\d{1,3})[-*?]$').match(i)
            return i if m is None else int(m.group(1) + '0')


class HyperlinkRelevanceHeuristicSorter:
    '''
    Sorts a list of hyperlinks in order of decreasing relevance based on the scoring heuristic.
    '''

    def __init__(self, heuristics, links):
        '''
        heuristics - a dictionary consisting of the following keys:
        - "host": a string retrieved from the netloc property of the return value of urllib.parse.urlparse
        - "identifier": the local identifier portion of the OAI identifier as described in http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm if the identifier is structured that way, otherwise the entire OAI identifier
        links - a list of HTTP URLs
        '''
        self.host = heuristics['host']
        self.identifier = heuristics['identifier']

        self.links = self.__heuristicSort(links)


    def mostRelevant(self):
        '''Return the most relevant link.'''

        return self.links[0]


    def rest(self):
        '''Return the rest of the links.'''

        return self.links[1:]


    def __heuristicSort(self, links):
        '''
        Sort links based on a heuristic for relevance.
        '''

        scores = {}
        for link in links:
            scores[link] = self.__score(link)

        links.sort(key=lambda x: scores[x], reverse=True)
        return links


    def __score(self, link):
        '''
        Highest scoring links are most relevant.
        '''

        score = 0
        if self.identifier in link:
            score += 1
        netloc = urllib.parse.urlparse(link).netloc
        if self.host == netloc:
            score += 1
        return score


class PRRLATinyDB:
    '''Helper class for simplifying interactions with the TinyDB instance.'''

    def __init__(self, path):
        self.db = TinyDB(path)

    def insert_or_update(self, collection_key, collection_name, institution_key, institution_name, resourcelist_uri, changelist_uri, url_map_from, resource_dir='resourcesync', overwrite=False):
        '''
        Add or update a single row.
        '''
        Row = Query()
        if not self.contains_set(institution_key, collection_key):
            # NOTE: if either `collection_key` or `institution_key` change for any given collection,
            # the filesystem location of the saved files will also change,
            # since resources are saved under the path `file_path_map_to`/`institution_key`/`collection_key`.

            # TODO: keep track of potential stale directories so they can be manually deleted.
            # This could involve logging calls to `add` where a row in the DB doesn't match the `institution_key` and `collection_key` parameters,
            # but DOES match either 1) both the `institution_name` and `collection_name` parameters, or 2) one of the URI parameters.

            self.db.insert({
                'collection_key': collection_key,
                'collection_name': collection_name,
                'institution_key': institution_key,
                'institution_name': institution_name,
                'resourcelist_uri': resourcelist_uri,
                'changelist_uri': changelist_uri,
                'url_map_from': url_map_from,
                'file_path_map_to': resource_dir,
                'new': True
                })
        elif overwrite == True:

            # TODO: if `file_path_map_to` changes, then we need to do a baseline synchronization again,
            # because that means the files will change location on the filesystem.
            # However, `file_path_map_to` should not be changed once chosen.
            self.db.update({
                'collection_key': collection_key,
                'collection_name': collection_name,
                'institution_key': institution_key,
                'institution_name': institution_name,
                'resourcelist_uri': resourcelist_uri,
                'changelist_uri': changelist_uri,
                'url_map_from': url_map_from,
                'file_path_map_to': resource_dir,
                }, Row.institution_key == institution_key and Row.collection_key == collection_key)
        else:
            # If row already exists and we don't want to overwrite, no-op.
            # TODO: log
            pass

    def remove_collections(self, institution_key, collection_keys=None):
        '''Remove rows from the database.'''
        Row = Query()
        if (collection_key is None):
            # Remove all collections
            self.db.remove(Row.institution_key == institution_key)
        else:
            # Remove only specified collections
            for collection_key in collection_keys:
                self.db.remove(Row.institution_key == institution_key and Row.collection_key == collection_key)

    def institution(self, institution_key):
        '''Lists all rows in the database.'''
        Row = Query()
        print(dumps(self.db.search(Row.institution_key == institution_key), indent=4))

    def show_all(self):
        print(dumps(self.db.all(), indent=4))

    def import_collections(self, resourcesync_sourcedescription, oaipmh_endpoint, collections_subset=None, **kwargs):
        '''
        Add to the database all of the resource sets specified in a ResourceSync SourceDescription.

        resourcesync_sourcedescription - a ResourceSync SourceDescription URL
        oaipmh_endpoint - the value of the "baseURL" field in the OAI-PMH Identify request, as specified in https://www.openarchives.org/OAI/openarchivesprotocol.html#Identify
        collections_subset - a list of collections to restrict the import to

        Keyword arguments:
            resource_dir - path to the local directory to store copies of the synced resources to, relative to the home directory "~"
            overwrite - whether or not to overwrite rows in the database that match the `collection_key` and `institution_key`
        '''
        rsSoup = BeautifulSoup(get(resourcesync_sourcedescription).content, 'xml')
        capabilitylist_urls = [a.string for a in rsSoup.find_all('loc')]

        sickle = Sickle(oaipmh_endpoint)
        sets = sickle.ListSets()
        identify = sickle.Identify()

        set_spec_to_name = {z.setSpec:z.setName for z in sets}
        url_map_from = '/'.join(oaipmh_endpoint.split(sep='/')[:-1]) + '/'

        for capabilitylist_url in capabilitylist_urls:

            # For now, get setSpec from the path component of the CapabilityList URL (which may have percent-encoded characters)
            set_spec = urllib.parse.unquote(urllib.parse.urlparse(capabilitylist_url).path.split(sep='/')[2])

            # If a subset of collections is specified, only add collections that belong to it. Otherwise, add all collections.
            if (collections_subset is None or (collections_subset is not None and set_spec in collections_subset)):

                r_soup = BeautifulSoup(get(capabilitylist_url).content, 'xml')

                # ResourceList should always exist, but if it doesn't, log it and skip this collection
                try:
                    resourcelist_url = r_soup.find(functools.partial(self.has_capability, 'resourcelist')).loc.string
                except AttributeError:
                    # TODO: log it
                    pass
                    continue

                # If no ChangeList exists yet, that's ok; predict what its URL will be
                try:
                    changelist_url = r_soup.find(functools.partial(self.has_capability, 'changelist')).loc.string
                except AttributeError:
                    changelist_url = '/'.join(resourcelist_url.split(sep='/')[:-1] + ['changelist_0000.xml'])

                print(set_spec, identify.repositoryName, identify.repositoryIdentifier)
                # We can add the collection to the database now
                self.insert_or_update(
                    set_spec,
                    set_spec_to_name[set_spec],
                    identify.repositoryIdentifier,
                    identify.repositoryName,
                    resourcelist_url,
                    changelist_url,
                    url_map_from,
                    **kwargs
                    )

    def has_capability(self, c, tag):
        return tag.md is not None and 'capability' in tag.md.attrs and tag.md['capability'] == c

    def contains_set(self, institution_key, collection_key):
        Row = Query()
        return self.db.contains(Row.institution_key == institution_key and Row.collection_key == collection_key)

def main():
    pass

if __name__ == '__main__':
    main()
