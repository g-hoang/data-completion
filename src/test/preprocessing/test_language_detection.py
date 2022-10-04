from unittest import TestCase

from src.preprocessing.language_detection import LanguageDetector


class Test(TestCase):
    def test_check_language_is_not_english(self):

        ld = LanguageDetector()

        self.assertTrue(ld.check_language_is_not_english('Бумажный дом 4 сезон 5 серия – сериал NETFLIX'))
        self.assertFalse(ld.check_language_is_not_english('Choice'))
        self.assertFalse(ld.check_language_is_not_english(['Choice', 'Merci']))
        self.assertFalse(ld.check_language_is_not_english('1999'))




