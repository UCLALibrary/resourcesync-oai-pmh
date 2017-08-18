import unittest
import rs_oaipmh_dest

class TestDestination(unittest.TestCase):
    def test_dateProcessingFunctions(self):
        dates = [
            '[188-?]',
            'c1904',
            '[1889?]',
            '1900]',
            '1903], c1895',
            '1973-08',
            '1956-09-07',
            'ca 1904',
            '54 C.E.',
            '500 B.C.E, ca 501',
            '2013-01-01T08:00:00Z'
            ]
        normalizedDates = [
            {1880},
            {1904},
            {1889},
            {1900},
            {1895, 1903},
            {1973},
            {1956},
            {1904},
            {54},
            {500, 501},
            {2013}
            ]
        decades = [
            {1880},
            {1900},
            {1880},
            {1900},
            {1890, 1900},
            {1970},
            {1950},
            {1900},
            {50},
            {500},
            {2010}
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
