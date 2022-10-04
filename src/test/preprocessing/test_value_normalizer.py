from unittest import TestCase

import phonenumbers

from src.preprocessing.value_normalizer import normalize_value


class Test(TestCase):
    def test_normalize_value(self):
        """Test Value normalizer"""
        # Setup - Date
        str_date_1 = '9.02.1901'
        str_date_2 = '1901/09/02'
        str_date_3 = '9-feb-1901'
        str_date_4 = '1901'
        str_date_5 = 'Neunzehnhundert'
        str_date_6 = 'June 4, 2004'
        str_date_7 = '2001-11-17T00:14:00+00:00'
        str_date_8 = '1999-12-05T08:00:00.000Z'
        str_date_9 = '2020-07-15'

        #str_date_9 = 'Thu Aug 03 2017 00:00:00 GMT+0200 (GMT+02:00)'

        # Run Tests - Date
        self.assertEqual('1901-09-02', normalize_value(str_date_1, 'date', None))
        self.assertEqual('1901-09-02', normalize_value(str_date_2, 'date', None))
        self.assertEqual('1901-02-09', normalize_value(str_date_3, 'date', None))
        self.assertEqual('1901-01-01', normalize_value(str_date_4, 'date', None))
        self.assertEqual(str_date_5, normalize_value(str_date_5, 'date', None))
        self.assertEqual('2004-06-04', normalize_value(str_date_6, 'date', None))
        self.assertEqual('2001-11-17', normalize_value(str_date_7, 'date', None))
        self.assertEqual('1999-12-05', normalize_value(str_date_8, 'date', None))
        self.assertEqual('2020-07-15', normalize_value(str_date_9, 'date', None))
        #self.assertEqual('2017-08-03', normalize_value(str_date_9, 'date', None))

        # Setup - Duration
        str_duration_1 = '12hr'
        str_duration_2 = '12hr5m10s'
        str_duration_3 = 'PT12H5M10S'
        str_duration_4 = 'PT120M'
        str_duration_5 = 'PT2H'
        str_duration_6 = '120 min'
        str_duration_7 = 'PTX'
        str_duration_8 = 'P95M'
        str_duration_9 = '1:33'
        str_duration_10 = '120'
        str_duration_11 = 'PTH2M28'


        # Run Tests - Duration


        self.assertEqual('PT12H', normalize_value(str_duration_1, 'duration', None))
        self.assertEqual('PT12H5M10S', normalize_value(str_duration_2, 'duration', None))
        self.assertEqual('PT12H5M10S', normalize_value(str_duration_3, 'duration', None))
        self.assertEqual('PT2H', normalize_value(str_duration_4, 'duration', None))
        self.assertEqual('PT2H', normalize_value(str_duration_5, 'duration', None))
        self.assertEqual('PT2H', normalize_value(str_duration_6, 'duration', None))
        self.assertEqual(str_duration_7, normalize_value(str_duration_7, 'duration', None))
        self.assertEqual('PT1H35M', normalize_value(str_duration_8, 'duration', None))
        self.assertEqual('PT1H33M', normalize_value(str_duration_9, 'duration', None))
        self.assertEqual('PT2H', normalize_value(str_duration_10, 'duration', None))
        self.assertEqual('PT2H28M', normalize_value(str_duration_11, 'duration', None))

        # Setup - Geo Coordinates
        str_coordinate_1 = '4.9121401E1'
        str_coordinate_2 = '49.121401'
        str_coordinate_3 = '-49.121401'
        str_coordinate_4 = 'E'
        str_coordinate_5 = 'E1'
        str_coordinate_6 = '-49,121401'
        str_coordinate_7 = '1.157068E1'
        str_coordinate_8 = '-8.65172E-2'


        # Run Tests - Geo Coordinates
        self.assertEqual('49.121401', normalize_value(str_coordinate_1, 'coordinate', None))
        self.assertEqual('49.121401', normalize_value(str_coordinate_2, 'coordinate', None))
        self.assertEqual('-49.121401', normalize_value(str_coordinate_3, 'coordinate', None))
        self.assertEqual(str_coordinate_4, normalize_value(str_coordinate_4, 'coordinate', None))
        self.assertEqual(str_coordinate_5, normalize_value(str_coordinate_5, 'coordinate', None))
        self.assertEqual('-49.121401', normalize_value(str_coordinate_6, 'coordinate', None))
        self.assertEqual('11.57068', normalize_value(str_coordinate_7, 'coordinate', None))
        self.assertEqual('-0.086517', normalize_value(str_coordinate_8, 'coordinate', None))

        # Setup - Geo Coordinates
        str_telephone_1 = '+49 69 717120'
        str_telephone_2 = '+1 212-490-8900'
        str_telephone_3 = '+'
        str_telephone_4 = '02124908900'
        str_telephone_5 = 'Phone:'
        str_telephone_6 = '0(212) 4908 - 900'
        str_telephone_7 = '0'
        str_telephone_8 = '0000'
        str_telephone_9 = 'Связаться с баней (сауной), получить дополнительную информацию по телефону: +996 555-81-91-04;'
        str_telephone_10 = '0044 0191 5193351'
        str_telephone_11 = '210 - 495 - 3900'
        str_telephone_12 = '0012104953900'
        entity_1 = {'address': {'addresscountry': 'US'}}
        entity_2 = {'address': {'addresscountry': 'USA'}}


        # Run Tests - Geo Coordinates
        self.assertEqual('+4969717120', normalize_value(str_telephone_1, 'telephone', None))
        self.assertEqual('+12124908900', normalize_value(str_telephone_2, 'telephone', None))
        self.assertEqual('', normalize_value(str_telephone_3, 'telephone', None))
        self.assertEqual(str_telephone_4, normalize_value(str_telephone_4, 'telephone', None))
        self.assertEqual('', normalize_value(str_telephone_5, 'telephone', None))
        self.assertEqual('02124908900', normalize_value(str_telephone_6, 'telephone', None))
        self.assertEqual('', normalize_value(str_telephone_7, 'telephone', None))
        self.assertEqual('', normalize_value(str_telephone_8, 'telephone', None))
        self.assertEqual('+996555819104', normalize_value(str_telephone_9, 'telephone', None))
        self.assertEqual('004401915193351', normalize_value(str_telephone_10, 'telephone', None))
        self.assertEqual('2104953900', normalize_value(str_telephone_11, 'telephone', None))
        self.assertEqual('+12104953900', normalize_value(str_telephone_11, 'telephone', entity_1))
        self.assertEqual('0012104953900', normalize_value(str_telephone_12, 'telephone', None))

        # Setup - Geo Coordinates
        str_country_1 = 'US'
        str_country_2 = 'USA'
        str_country_3 = 'Germany'
        str_country_4 = 'Germania'
        str_country_5 = 'DEU'
        str_country_6 = None


        # Run Tests - Country
        self.assertEqual('US', normalize_value(str_country_1, 'country', None))
        self.assertEqual('US', normalize_value(str_country_2, 'country', None))
        self.assertEqual('DE', normalize_value(str_country_3, 'country', None))
        self.assertEqual('Germania', normalize_value(str_country_4, 'country', None))
        self.assertEqual('DE', normalize_value(str_country_5, 'country', None))
        self.assertEqual('None', normalize_value(str_country_6, 'country', None))
