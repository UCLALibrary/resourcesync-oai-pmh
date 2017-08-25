import unittest
import sys
import functools
import pdb
import traceback
import logging
import rs_oaipmh_dest

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s\t%(levelname)s\t%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
    )

class TestDestination(unittest.TestCase):
    def test_resolveUnknownOnes(self):
        self.assertEqual(rs_oaipmh_dest.resolveUnknownOnes('1998?'), 1998)
        self.assertEqual(rs_oaipmh_dest.resolveUnknownOnes('199-?'), 1990)
        self.assertEqual(rs_oaipmh_dest.resolveUnknownOnes('199?'), 1990)
        self.assertEqual(rs_oaipmh_dest.resolveUnknownOnes('19-?'), 190)
        self.assertEqual(rs_oaipmh_dest.resolveUnknownOnes('1-?'), 10)

    def test_dateProcessingFunctions(self):
        dates = [
            '[186-?]',
            'c1904',
            '[1899?]',
            '1900]',
            '1903], c1895',
            '1973-08',
            '1956-09-07',
            'ca 1904',
            '500 BC',
            '2013-01-01T08:00:00Z',
            '1922-1927',
            '1903?',
            '2004/5',
            '1959 1960',
            '13-Mar-76',
            '1972 1973 1974 1975 1976 Date notes: Digital photos created 2002. Pottery found 1972-1976',
            'Feb-76',
            '1300-1200 BC',
            '2nd C BC',
            '3rd C AD',
            '1993-03 - 1993-05',
            '4th C  AD',
            '2800 BC [ca.]',
            '447-432 BC',
            '1965-1969?',
            '1978-03/ 1978-10',
            'c1963',
            'pre 1993/4',
            '07 Mar 1976. 7.30pm',
            '1500 [ca.]',
            '1970s',
            '1851, modified 1853-1854',
            'c. 470-460 BC',
            '2550-2530 BC [ca.]',
            'c.1926',
            '1980-03/1980-07',
            '12 Mar 1976. 2.00am'
            ]
        normalizedDates = [
            {1860},
            {1904},
            {1899},
            {1900},
            {1895, 1903},
            {1973},
            {1956},
            {1904},
            {-500},
            {2013},
            {1922, 1927},
            {1903},
            {2004},
            {1959, 1960},
            {1976},
            {1972, 1973, 1974, 1975, 1976, 2002},
            {1976},
            {-1300, -1200},
            {-200, -101},
            {200, 299},
            {1993},
            {300, 399},
            {-2800},
            {-447, -432},
            {1965, 1969},
            {1978},
            {1963},
            {1993},
            {1976},
            {1500},
            {1970},
            {1851, 1853, 1854},
            {-470, -460},
            {-2550, -2530},
            {1926},
            {1980},
            {1976}
            ]
        decades = [
            {1860},
            {1900},
            {1890},
            {1900},
            {1890, 1900},
            {1970},
            {1950},
            {1900},
            {-500},
            {2010},
            {1920},
            {1900},
            {2000},
            {1950, 1960},
            {1970},
            {1970, 1980, 1990, 2000},
            {1970},
            {-1300, -1290, -1280, -1270, -1260, -1250, -1240, -1230, -1220, -1210, -1200},
            {-200, -190, -180, -170, -160, -150, -140, -130, -120, -110},
            {200, 210, 220, 230, 240, 250, 260, 270, 280, 290},
            {1990},
            {300, 310, 320, 330, 340, 350, 360, 370, 380, 390},
            {-2800},
            {-450, -440},
            {1960},
            {1970},
            {1960},
            {1990},
            {1970},
            {1500},
            {1970},
            {1850},
            {-470, -460},
            {-2550, -2540, -2530},
            {1920},
            {1980},
            {1970}
            ]

        for i in range(0, len(dates)):
            self.assertEqual(rs_oaipmh_dest.cleanAndNormalizeDate(dates[i]), normalizedDates[i])
            self.assertEqual(rs_oaipmh_dest.facet_decades(normalizedDates[i]), decades[i])

    def test_addValuePossiblyDuplicateKey(self):
        testDict = {}

        rs_oaipmh_dest.addValuePossiblyDuplicateKey('1', 'test', testDict)
        self.assertEqual(testDict['1'], 'test')

        rs_oaipmh_dest.addValuePossiblyDuplicateKey('1', 'test', testDict)
        self.assertEqual(testDict['1'], ['test', 'test'])

        rs_oaipmh_dest.addValuePossiblyDuplicateKey('1', 'test', testDict)
        self.assertEqual(testDict['1'], ['test', 'test', 'test'])

if __name__ == '__main__':
    unittest.main()
