from unittest import TestCase

from src.model.querytable import load_query_table_from_file, load_query_tables, QueryTable


class TestQuerytable(TestCase):

    def test_load_query_table_from_file(self):
        # Setup
        path_to_query_table = 'data/querytables/test_movie/harry_potter/gs_querytable_harry_potter_director_10.json'
        querytable = load_query_table_from_file(path_to_query_table)
        querytable.verified_evidences.sort(key=lambda Evidence: Evidence.identifier)
        # Test
        self.assertEqual(10, querytable.identifier)
        self.assertEqual("movie", querytable.schema_org_class)
        self.assertEqual("director", querytable.target_attribute)
        self.assertEqual(type(querytable.verified_evidences), list)
        self.assertEqual(3, querytable.verified_evidences[0].scale)

    def test_load_query_tables(self):
        # Setup
        querytables = load_query_tables()
        no_loaded_querytables = len([querytable for querytable in querytables if type(querytable) is QueryTable])

        # Test
        self.assertEqual(type(querytables), list)
        self.assertEqual(no_loaded_querytables, len(querytables))

    def test_to_json(self):
        # Setup
        path_to_query_table = 'data/querytables/test_movie/harry_potter/gs_querytable_harry_potter_director_10.json'
        querytable = load_query_table_from_file(path_to_query_table)

        json_querytable = querytable.to_json(with_evidence_context=False)

        self.assertEqual(10, json_querytable['id'])

    def test_remove_context_attribute(self):
        # Setup
        path_to_query_table = 'data/querytables/test_movie/harry_potter/gs_querytable_harry_potter_director_10.json'
        querytable = load_query_table_from_file(path_to_query_table)

        # Try to remove name attribute --> Not allowed!
        with self.assertRaises(ValueError):
            querytable.remove_context_attribute('name')

        # Try to remove not existing context attribute --> not possible!
        with self.assertRaises(ValueError):
            querytable.remove_context_attribute('address')

        # Remove context attribute
        querytable.remove_context_attribute('datepublished')
        self.assertNotIn('datepublished', querytable.context_attributes)
        self.assertNotIn('datepublished', querytable.table[0])
