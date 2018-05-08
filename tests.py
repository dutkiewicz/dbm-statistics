import unittest
from datetime import datetime
from core.util import DBMQuery, clean_currency_value, clean_date_value


class CleanCurrenyValueTest(unittest.TestCase):
    """
    Test functions from core.util for clean_currency_value
    """

    def test_is_NaN(self):
        """
        Does raise ValueError for non-numerical values?
        :return:
        """
        value = 'twenty dollars'
        with self.assertRaises(ValueError):
            clean_currency_value(value)

    def test_space_separated(self):
        """
        Does string have space separator for hundreds and still return float?
        :return:
        """
        value = '1 234 567'
        target = 1234567.0
        self.assertEqual(clean_currency_value(value), target)

    def test_comma_separated(self):
        """
        Does string have space comma for hundreds and still return float?
        :return:
        """
        value = '1,234,567'
        target = 1234567.0
        self.assertEqual(clean_currency_value(value), target)

    def test_currency_ascii(self):
        """
        Does string with currency annotation is cleaned to float?
        :return:
        """
        value = 'PLN100.001'
        target = 100.001
        self.assertEqual(clean_currency_value(value), target)

    def test_currency_symbols(self):
        """
        Does string with utf8 currency annotation is cleaned to float?
        :return:
        """
        value = u'\u20AC100.001'
        target = 100.001
        self.assertEqual(clean_currency_value(value), target)


class CleanDateValueTest(unittest.TestCase):
    """
    Does parsing date string return correct datetime object?
    """

    def test_YYYY_slash_mm_slash_dd(self):
        """Test format %Y/%m/%d"""

        value = '2018/01/01'
        target = datetime(2018, 1, 1)

        self.assertEqual(clean_date_value(value), target)

    def test_YYYY_dash_mm_dash_dd(self):
        """Test format %Y-%m-%d"""

        value = '2018-01-01'
        target = datetime(2018, 1, 1)

        self.assertEqual(clean_date_value(value), target)

    def test_YYYY_dot_mm_dot_dd(self):
        """Test format %Y.%m.%d"""

        value = '2018.01.01'
        target = datetime(2018, 1, 1)

        self.assertEqual(clean_date_value(value), target)

    def test_YYYY_slash_dd_slash_mm(self):
        """Test format %Y/%d/%m"""

        value = '2018/30/01'
        target = datetime(2018, 1, 30)

        self.assertEqual(clean_date_value(value), target)

if __name__ == '__main__':
    unittest.main()