from unittest import TestCase

from src.similarity.string_comparator import string_similarity, string_containment


class Test(TestCase):
    def test_string_similarity(self):

        # Setup
        test_strings1 = ('Marriott Frankfurt', 'Marriott Frankfurt')
        test_strings2 = ('Mariott Frankfurt', 'Marriott Frankfurt')
        test_strings3 = ('Marriott Munich', 'Marriott Frankfurt')
        test_strings4 = ('Marriott Frankfurt', 'Marriott Frankfurt, Hessen')
        test_strings5 = ('3B Architecture Nancy France', '3B Architecture Nancy')
        test_strings6 = ('Alfred Coffee', 'Alfred Coffee in the Alley West Hollywood')

        # Execute Tests
        self.assertTrue(string_similarity(test_strings1[0], test_strings1[1]) == 1)
        self.assertTrue(string_similarity(test_strings2[0], test_strings2[1]) > 0.9)
        self.assertTrue(string_similarity(test_strings3[0], test_strings3[1]) < 0.9)
        self.assertTrue(string_similarity(test_strings4[0], test_strings4[1]) > 0.7)
        self.assertTrue(string_similarity(test_strings5[0], test_strings5[1]) > 0.7)
        self.assertTrue(string_similarity(test_strings6[0], test_strings6[1]) < 0.7)

    def test_string_containment(self):

        # Setup
        test_strings1 = ('Alfred Coffee', 'Alfred Coffee in the Alley West Hollywood')
        test_strings2 = ('Alley West Hollywood Alfred Coffee', 'Alfred Coffee in the Alley West Hollywood')
        test_strings3 = ('Five Guys	', 'Five Guys Port Charlotte')
        test_strings4 = ('400 Gradi	', '400 Gradi Eastland Ringwood')
        test_strings5 = ('Mercante London	', 'Mercante London')


        # Execute Tests
        self.assertTrue(string_containment(test_strings1[0], test_strings1[1]))
        self.assertTrue(string_containment(test_strings2[0], test_strings2[1]))
        self.assertTrue(string_containment(test_strings3[0], test_strings3[1]))
        self.assertTrue(string_containment(test_strings4[0], test_strings4[1]))
        self.assertTrue(string_containment(test_strings5[0], test_strings5[1]))
