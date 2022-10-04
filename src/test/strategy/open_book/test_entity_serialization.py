from unittest import TestCase

from src.strategy.open_book.entity_serialization import preprocess_attribute_value, EntitySerializer


class Test(TestCase):
    def test_preprocess_attribute_value(self):
        # Setup
        entity_1 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona', 'addressregion': None,
                  'streetaddress': ['8', 'Polígono Industrial']}
        name_entity_1_processed = 'TRANSGRUAS CIAL, SL'
        addresslocality_entity_1_processed = 'Barcelona'
        addressregion_entity_1_processed = None
        streetaddress_entity_1_processed = '8, Polígono Industrial'

        entity_2 = None

        # Test
        self.assertEqual(preprocess_attribute_value(entity_1, 'name'), name_entity_1_processed)
        self.assertEqual(preprocess_attribute_value(entity_1, 'addresslocality'), addresslocality_entity_1_processed)
        self.assertEqual(preprocess_attribute_value(entity_1, 'addressregion'), addressregion_entity_1_processed)
        self.assertEqual(preprocess_attribute_value(entity_1, 'streetaddress'), streetaddress_entity_1_processed)

        # Test exception --> No entity supplied!
        with self.assertRaises(ValueError) as context:
            preprocess_attribute_value(entity_2, 'name')
            self.assertEqual(context.exception, ValueError('Entity must not be None!'))

    def test_convert_to_str_representation(self):
        # Setup
        schema_org_class = 'localbusiness'
        entity_serializer = EntitySerializer(schema_org_class)
        entity_1 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona', 'addressregion': None,
                  'streetaddress': ['8', 'Polígono Industrial']}
        serialized_entity_1 = '[COL]name[VAL]TRANSGRUAS CIAL, SL[COL]addresslocality[VAL]Barcelona[COL]streetaddress[VAL]8, Polígono Industrial'

        # Remove only streetaddress - addressregion is None and director is not present in localbusiness schema
        exclude_attributes_1 = ['addressregion', 'streetaddress', 'director']
        serialized_entity_2 = '[COL]name[VAL]TRANSGRUAS CIAL, SL[COL]addresslocality[VAL]Barcelona'

        entity_2 = None

        # Test
        self.assertEqual(entity_serializer.convert_to_str_representation(entity_1), serialized_entity_1)
        self.assertEqual(entity_serializer.convert_to_str_representation(entity_1, exclude_attributes_1),
                         serialized_entity_2)
        with self.assertRaises(ValueError) as context:
            entity_serializer.convert_to_str_representation(entity_2)
            self.assertEqual(context.exception, ValueError('Entity must not be None!'))

    def test_convert_to_cross_encoder_representation(self):
        # Setup
        schema_org_class = 'localbusiness'
        entity_serializer = EntitySerializer(schema_org_class)
        entity_1 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona', 'addressregion': None,
                  'streetaddress': ['8', 'Polígono Industrial']}
        entity_2 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona', 'addressregion': None,
                  'streetaddress': ['8', 'Polígono Industrial']}
        cross_encoder_serialization = '[COL]name[VAL]TRANSGRUAS CIAL, SL[COL]addresslocality[VAL]Barcelona[' \
                                      'COL]streetaddress[VAL]8, Polígono Industrial[SEP][COL]name[VAL]TRANSGRUAS ' \
                                      'CIAL, SL[COL]addresslocality[VAL]Barcelona[COL]streetaddress[VAL]8, ' \
                                      'Polígono Industrial'
        # Test
        self.assertEqual(entity_serializer.convert_to_cross_encoder_representation(entity_1, entity_2),
                         cross_encoder_serialization)

    def test_project_entity(self):
        # Setup
        schema_org_class = 'localbusiness'
        entity_serializer = EntitySerializer(schema_org_class)

        entity_1 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona', 'addressregion': None,
                  'streetaddress': ['8', 'Polígono Industrial']}
        test_projected_entity_1 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona',
                                'streetaddress': '8, Polígono Industrial'}
        excluded_attributes_1 = ['addressregion', 'streetaddress']
        selected_attributes_1 = ['name', 'addresslocality']
        test_projected_entity_2 = {'name': 'TRANSGRUAS CIAL, SL', 'addresslocality': 'Barcelona'}

        entity_2 = None

        # Test - Simple Projection
        projected_entity_1 = entity_serializer.project_entity(entity_1)
        for key in projected_entity_1:
            self.assertEqual(projected_entity_1[key], test_projected_entity_1[key])
        self.assertEqual(len(projected_entity_1.keys()), len(test_projected_entity_1.keys()))

        # Test - Exclude attributes
        projected_entity_2 = entity_serializer.project_entity(entity_1, excluded_attributes=excluded_attributes_1)
        for key in projected_entity_2:
            self.assertEqual(projected_entity_2[key], test_projected_entity_2[key])
        self.assertEqual(len(projected_entity_2.keys()), len(test_projected_entity_2.keys()))

        # Test - Select attributes
        projected_entity_2 = entity_serializer.project_entity(entity_1, selected_attributes=selected_attributes_1)
        for key in projected_entity_2:
            self.assertEqual(projected_entity_2[key], test_projected_entity_2[key])
        self.assertEqual(len(projected_entity_2.keys()), len(test_projected_entity_2.keys()))

        # Test - Select and exclude attributes --> Should not be possible
        with self.assertRaises(ValueError) as context:
            entity_serializer.project_entity(entity_1, excluded_attributes=excluded_attributes_1,
                                             selected_attributes=selected_attributes_1)
            self.assertEqual(context.exception, ValueError('Either select or exclude attributes - Do not do both!'))

        # Test Supply empty entity --> Should not be possible
        with self.assertRaises(ValueError) as context:
            entity_serializer.project_entity(entity_2)
            self.assertEqual(context.exception, ValueError('Entity must not be None!'))
