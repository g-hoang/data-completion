from unittest import TestCase

from src.model.evidence import Evidence


class TestEvidence(TestCase):
    def test_verified_signals(self):
        # Setup
        evidence = Evidence(identifier=1, query_table_id= 11, entity_id=0, value='David Yates',
                            table= 'movie_putlockers.app_september2020.json.gz', row_id=50,
                            attribute='director', context= {})

        # Run test
        evidence.verify(True)
        self.assertTrue(evidence.signal)

        evidence.verify(False)
        self.assertFalse(evidence.signal)

        with self.assertRaises(ValueError) as context:
            evidence.verify(None)

            self.assertTrue('The value of signal must be defined (True/False)!' in context.exception)

