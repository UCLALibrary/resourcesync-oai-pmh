import unittest
import sys
import functools
import pdb
import traceback
import logging
from resourcesync_oai_pmh.destination.util import DateCleanerAndFaceter, HyperlinkRelevanceHeuristicSorter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
    )
logger = logging.getLogger('root')

class TestUtil(unittest.TestCase):

    def test_DateCleanerAndFaceter(self):
        dates = [
            ('[186-?]', {1860}),
            ('c1904', {1900}),
            ('[1899?]', {1890}),
            ('1900]', {1900}),
            ('1903], c1895', {1890, 1900}),
            ('1973-08', {1970}),
            ('1956-09-07', {1950}),
            ('ca 1904', {1900}),
            ('500 BC', {-500}),
            ('2013-01-01T08:00:00Z', {2010}),
            ('1922-1927', {1920}),
            ('1903?', {1900}),
            ('2004/5', {2000}),
            ('1959 1960', {1950, 1960}),
            ('13-Mar-76', {1970}),
            ('1972 1973 1974 1975 1976 Date notes: Digital photos created 2002. Pottery found 1972-1976', {1970, 2000}),
            ('Feb-76', {1970}),
            ('1300-1200 BC', set(range(-1300, -1200 + 1, 10))),
            ('2nd C BC', {-200, -190, -180, -170, -160, -150, -140, -130, -120, -110}),
            ('3rd C AD', {200, 210, 220, 230, 240, 250, 260, 270, 280, 290}),
            ('1993-03 - 1993-05', {1990}),
            ('4th C  AD', {300, 310, 320, 330, 340, 350, 360, 370, 380, 390}),
            ('2800 BC [ca.]', {-2800}),
            ('447-432 BC', {-450, -440}),
            ('1965-1969?', {1960}),
            ('1978-03/ 1978-10', {1970}),
            ('c1963', {1960}),
            ('pre 1993/4', {1990}),
            ('07 Mar 1976. 7.30pm', {1970}),
            ('1500 [ca.]', {1500}),
            ('1970s', {1970}),
            ('1851,  modified 1853-1854',{1850}),
            ('c. 470-460 BC', {-470, -460}),
            ('2550-2530 BC [ca.]', {-2550, -2540, -2530}),
            ('c.1926', {1920}),
            ('1980-03/1980-07', {1980}),
            ('12 Mar 1976. 2.00am', {1970}),
            ('1600-1040 BC', set(range(-1600, -1040 + 1, 10))),
            ('1600-1046 BC', set(range(-1600, -1050 + 1, 10))),
            ('1600 BC - 1046 BC', set(range(-1600, -1050 + 1, 10))),
            ('1600 BC-1046 BC', set(range(-1600, -1050 + 1, 10))),
            ('Notamonth 11 (1968)', {1960}),
            ('Notamonth 46 (1968)', {1960})
            ]

        for i in range(0, len(dates)):
            self.assertEqual(
                DateCleanerAndFaceter(dates[i][0]).decades(),
                dates[i][1])

    def test_HyperlinkRelevanceHeuristicSorter(self):
        links = [
            (
                {
                    'host': 'repository.x.y.edu',
                    'identifier': 'aaa-1000'
                },
                [
                    'http://archives.x.y.edu/repositories/1/archival_objects/aaa-1000',
                    'http://repository.x.y.edu/en/item/aaa-1000'
                ],
                'http://repository.x.y.edu/en/item/aaa-1000'
            ),
            (
                {
                    'host': 'repository.x.y.edu',
                    'identifier': 'bbb-1000'
                },
                [
                    'http://repository.x.y.edu/en/item/bbb-1000'
                ],
                'http://repository.x.y.edu/en/item/bbb-1000'
            )
        ]

        for i in range(0, len(links)):
            hrhs = HyperlinkRelevanceHeuristicSorter(links[i][0], links[i][1]) 
            self.assertEqual(
                hrhs.mostRelevant(),
                links[i][2])

if __name__ == '__main__':
    unittest.main()
