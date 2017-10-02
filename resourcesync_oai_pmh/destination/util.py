#!/usr/bin/python3

import collections
from datetime import date
from dateutil.parser import parse
from functools import reduce
from json import dumps
import logging
import logging.config
import os
import re
import sys


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
            'substitution': {}
            }
        self.regexes['match']['mm'] = r'(?:0[1-9]|1[0-2])'
        self.regexes['match']['dd'] = r'(?:0[1-9]|[1-2]\d|3[1-2])'
        self.regexes['match']['mon'] = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
        self.regexes['match']['time'] = r'\d{1,2}[.:]\d{2}(?:[ap]\.?m)?'

        # TODO: require a 2 or 3 digit year to have a suffix
        self.regexes['match']['year'] = r'[1-9]\d*'
        self.regexes['match']['year?'] = r'[1-9]\d*[-*?]?'

        # matches a year followed by a 2 digit month (must not be followed by another digit), the year can have a mystery one's place
        self.regexes['match']['year-mm'] = r'{}(?:(?:[-/]{})|[-*?])?(?=\D|$)'.format(
            self.regexes['match']['year'],
            self.regexes['match']['mm'])

        # matches a range of years, separated by either - or /
        # TODO: allow a range of years to have both ends end in a suffix
        self.regexes['match']['year-year'] = r'{}\s*[-/]\s*{}'.format(
            self.regexes['match']['year-mm'],
            self.regexes['match']['year-mm'])

        self.regexes['match']['dd-mon-year-time'] = r'{}\s+{}\s+{}(?:\.\s+{})?'.format(
            self.regexes['match']['dd'],
            self.regexes['match']['mon'],
            self.regexes['match']['year'],
            self.regexes['match']['time'])

        # not used
        #self.regexes['match']['year/m'] = r'{}/\d'.format(self.regexes['match']['year'])

        # matches a century string
        self.regexes['match']['century'] = r'(?:1st|2nd|3rd|(?:[4-9]|1[0-9]|20)th)\s+[cC](?:entury)?'

        # order of alternate patterns is important
        self.regexes['match']['alternator'] = r'(?:({})|({})|({})|({}))'.format(
            self.regexes['match']['century'],
            self.regexes['match']['year-year'],
            self.regexes['match']['dd-mon-year-time'],
            self.regexes['match']['year?'])

        self.regexes['match']['suffix-bce'] = r'BC|B\.C\.|BCE|B\.C\.E\.'
        self.regexes['match']['suffix-ce']= r'AD|A\.D\.|CE|C\.E\.'
        self.regexes['match']['suffix'] = r'(?:{}|{})'.format(
            self.regexes['match']['suffix-bce'],
            self.regexes['match']['suffix-ce'])

        self.regexes['match']['date'] = r'{}(?:\s+({}))?'.format(
            self.regexes['match']['alternator'],
            self.regexes['match']['suffix'])

        self.regexes['substitution']['year-year'] = r'({})\s*[-/]\s*({})'.format(
            self.regexes['match']['year-mm'],
            self.regexes['match']['year-mm'])

        self.regexes['substitution']['dd-mon-year-time'] = r'{}\s+{}\s+({})(?:\.\s+{})?'.format(
            self.regexes['match']['dd'],
            self.regexes['match']['mon'],
            self.regexes['match']['year'],
            self.regexes['match']['time'])


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
        Maps a match of regexes['match']['date'] to a set of years.

        m - the re.match object

        Match indices:
            0 -> 'century'
            1 -> 'year-year'
            2 -> 'dd-mon-year-time'
            3 -> 'year?'
            4 -> 'suffix'
        '''

        years = set()
        try:
            if m[0] != '':
                # year-range
                century = int(re.match(re.compile('\d+'), m[0]).group(0))
                years = (100 * (century - 1), 100 * (century - 1) + 99)
            elif m[1] != '':
                # year-range
                rangeOfStuff = [int(self.__resolveUnknownOnes(y.strip())) for y in re.sub(self.regexes['substitution']['year-year'], r'\1>|<\2', m[1]).split('>|<')]
                years = (rangeOfStuff[0], rangeOfStuff[1])
            elif m[2] != '':
                # year
                prep = re.sub(self.regexes['substitution']['dd-mon-year-time'], r'\1', m[2]).strip()
                years = int(self.__resolveUnknownOnes(prep))
            elif m[3] != '':
                # year
                years = int(self.__resolveUnknownOnes(m[3]))
            else:
                raise Error

        except ValueError as e:
            #logger.error('An error occurred while trying to match "{}": {}'.format(m, e))
            pass

        if m[4] != '' and re.compile(self.regexes['match']['suffix-bce']).match(m[4]) is not None:
            # move everything to the left side of year 0 on the timeline
            if m[0] != '':
                years = (100 * -century, 100 * -century + 99)
            elif m[1] != '':
                years = (-years[0], -years[1])
            else:
                years = -years

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

        m = re.compile('(\d{4})').match(i)
        if m is not None:
            return int(m.group(1))
        else:
            m = re.compile('(\d{1,3})[-*?]').match(i)
            return i if m is None else int(m.group(1) + '0')

