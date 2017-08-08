import unittest
import rs_oaipmh_dest

class TestDestination(unittest.TestCase):
    def test_cleanAndNormalizeDate(self):
        dates = [
            '[188-?]',
            'c1904',
            '[1889?]',
            '1900]',
            '1903], c1895',
            '1973-08',
            '1956-09-07',
            'ca 1904'
            ]
        normalizedDates = [
            '1880',
            '1904',
            '1889',
            '1900',
            '1903',
            '1973',
            '1956',
            '1904'
            ]

        # should handle all of these
        for i in range(0, len(dates)):
            #print(rs_oaipmh_dest.cleanAndNormalizeDate(dates[i]))
            self.assertEqual(rs_oaipmh_dest.cleanAndNormalizeDate(dates[i]), normalizedDates[i])

if __name__ == '__main__':
    unittest.main()
