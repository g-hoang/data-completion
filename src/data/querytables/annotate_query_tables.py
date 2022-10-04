import logging
import os
import random

import pandas as pd

from src.model.querytable_new import get_gt_tables, get_query_table_paths, load_query_table_from_file
from src.strategy.open_book.ranking.similarity.similarity_re_ranking_factory import select_similarity_re_ranker
from src.strategy.open_book.retrieval.query_by_goldstandard import QueryByGoldStandard


def annotate_query_tables_corner_cases(schema_org):
    logger = logging.getLogger()

    # Annotate retrieval tables
    categories = get_gt_tables('retrieval', schema_org)
    strategy_obj = QueryByGoldStandard(schema_org, clusters=True) # Retrieve training data from all possible entities

    for category in categories:
        query_table_paths = get_query_table_paths('retrieval', schema_org, category)
        query_tables = [load_query_table_from_file(path) for path in query_table_paths]

        # Determine corner cases of evidences using RF from Magellan
        #   Load three Magellan re-ranker
        re_ranking_strategy = {'name': 'magellan_re_ranker', 'model_name': 'RF'}
        # Use fixed context attribute combinations to be as consistent as possible across all query tables
        if schema_org == 'localbusiness':
            context_attribute_options = [['name'], ['name', 'addresslocality']]
                                     #['name', 'addresslocality', 'addressregion', 'addresscountry', 'postalcode',
                                     # 'streetaddress']]
        elif schema_org == 'product':
            context_attribute_options = [['name']]
        else:
            ValueError('Schema Org Class {} is not known'.format(schema_org))

        # Initialize three different magellan re-ranker
        mg_re_ranker = [select_similarity_re_ranker(re_ranking_strategy, schema_org, context_attributes)
                        for context_attributes in context_attribute_options]

        for query_table in query_tables:
            evidences = strategy_obj.retrieve_evidence(query_table, 100, None)
            for re_ranker in mg_re_ranker:
                re_ranked_evidences = re_ranker.re_rank_evidences(query_table, evidences)

            for evidence in re_ranked_evidences:
                # Identify corner cases
                if schema_org == 'product':
                    if sum([value for key, value in evidence.scores.items()]) == 2:
                        evidence.corner_case = False
                    else:
                        evidence.corner_case = True
                elif schema_org == 'localbusiness':
                    if sum([value for key, value in evidence.scores.items()]) == 3:
                        evidence.corner_case = False
                    else:
                        evidence.corner_case = True

            for row in query_table.table:
                found_corner_cases_per_row = sum([1 for evidence in re_ranked_evidences if
                                 evidence.entity_id == row['entityId'] and evidence.corner_case])
                evidences_per_row = sum([1 for evidence in re_ranked_evidences if
                                                  evidence.entity_id == row['entityId']])
                print('Found corner cases: {} out of {}'.format(found_corner_cases_per_row, evidences_per_row))

            query_table.save(with_evidence_context=False)

def annotate_query_tables_seen_during_training(schema_org):
    """Annotate entities (clusters), which have been seen during training"""
    logger = logging.getLogger()
    random.seed(42)

    path_to_fine_tuning_ds = '{}/finetuning/open_book/{}_fine-tuning_complete_subset_pairs.csv'.format(os.environ['DATA_DIR'], schema_org)
    dtypes = {'addresslocality': str, 'name': str}
    df_training_data = pd.read_csv(path_to_fine_tuning_ds, encoding='utf-8', sep=';', dtype=dtypes)
    logger.info('DS loaded with {} entities'.format(len(df_training_data)))

    known_entities = [(row['table'], row['row_id']) for index, row in df_training_data.iterrows()]

    # Annotate retrieval tables
    gt_tables = get_gt_tables('retrieval', schema_org)
    count_seen_entities = 0

    for gt_table in gt_tables:
        query_table_paths = get_query_table_paths('retrieval', schema_org, gt_table)
        query_tables = [load_query_table_from_file(path) for path in query_table_paths]

        for query_table in query_tables:
            for row in query_table.table:
                row_evidences = [evidence for evidence in query_table.verified_evidences if evidence.entity_id == row['entityId']]
                seen_during_training = False
                for evidence in row_evidences:
                    if (evidence.table, evidence.row_id) in known_entities:
                        seen_during_training = True
                        break

                if seen_during_training:
                    count_seen_entities += 1

                    for evidence in row_evidences:
                        evidence.seen_training = True
                else:
                    for evidence in row_evidences:
                        evidence.seen_training = False

            query_table.save(with_evidence_context=False)


    logger.info('Seen entities: {}'.format(count_seen_entities))


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    schema_org = 'product'

    annotate_query_tables_corner_cases(schema_org)
    annotate_query_tables_seen_during_training(schema_org)
