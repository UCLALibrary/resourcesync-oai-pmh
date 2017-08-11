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
            'ca 1904',
            '54 C.E.',
            '500 B.C.E, ca 501'
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
            {500, 501}
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
            {500}
            ]

        for i in range(0, len(dates)):
            self.assertEqual(rs_oaipmh_dest.cleanAndNormalizeDate(dates[i]), normalizedDates[i])
            self.assertEqual(rs_oaipmh_dest.facet_decade(normalizedDates[i]), decades[i])

if __name__ == '__main__':
    unittest.main()
