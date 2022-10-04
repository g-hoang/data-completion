import os
from unittest import TestCase

from src.model.querytable import load_query_tables_by_class, load_query_table_from_file
from src.strategy.open_book.ranking.similarity.magellan_re_ranker import MagellanSimilarityReRanker
from src.strategy.open_book.ranking.similarity.similarity_re_ranking_factory import select_similarity_re_ranker


class Test(TestCase):
    def test_magellan_re_ranker(self):
        # Setup
        #   Prepare Magellan re_ranker selection
        # TO-DO: CLEAN SETUP OF TEST CASE
        re_ranking_strategy_conf = {'name': 'magellan_re_ranker', 'model_name': 'RF'}
        schema_org = 'movie'
        magellan_re_ranker = select_similarity_re_ranker(re_ranking_strategy_conf, schema_org)

        # Make sure that the correct Reranker is loaded
        self.assertTrue(isinstance(magellan_re_ranker, MagellanSimilarityReRanker))

        # Load query tables
        category = 'harry_potter'
        file_name = 'gs_querytable_harry_potter_datepublished_12.json'
        path_to_test_query_table = '{}/querytables/{}/{}/{}'.format(os.environ['DATA_DIR'], schema_org, category,
                                                                    file_name)
        query_table = load_query_table_from_file(path_to_test_query_table)

        # Add dummy context to all verified evidences
        # TO-DO: Add real context!
        evidences = []
        for evidence in query_table.verified_evidences:
            evidence.context = {
                "datepublished": "2001-11-16",
                "director": "Chris Columbus",
                "name": "Harry Potter and the Sorcerer's Stone"
            }

            evidences.append(evidence)

        # To-Do: Implement Tests for re-ranking!
        #evidences = magellan_re_ranker.re_rank_evidences(query_table, query_table.verified_evidences)
