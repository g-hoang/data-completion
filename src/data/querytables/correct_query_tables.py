import os
import logging

import fasttext

from src.model.evidence import Evidence
from src.model.querytable_new import get_gt_tables, get_query_table_paths, load_query_table_from_file
from src.preprocessing.language_detection import LanguageDetector
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity


# def load_and_correct_goldstandard():
#     logger = logging.getLogger()
#
#     # Load query tables
#     reduced_rows = 0
#     total_rows = 0
#
#     schema_org = 'localbusiness'
#
#     categories = get_categories(schema_org)
#     #categories = ['accor_com']
#     strategy_obj = QueryByEntity(schema_org)
#
#     for category in categories:
#         query_table_paths = get_query_table_paths(schema_org, category)
#         query_tables = [load_query_table_from_file(path) for path in query_table_paths]
#         ooc_entities_by_assembling_strategy = {}
#         wrongly_assigned_entities = {}
#
#         evidence_per_entity_identifiers = {'head': {}, 'tail': {}}
#         # Populate evidences across query tables --> Assign None as value if evidence was not maintained earlier.
#         for query_table in query_tables:
#             entity_type = 'head' if 'head' in query_table.assembling_strategy else 'tail'
#
#             # Exclude ground truth tables
#             query_table.verified_evidences = strategy_obj.filter_evidences_by_ground_truth_tables(
#                 query_table.verified_evidences)
#
#             for evidence in query_table.verified_evidences:
#                 evidence_per_entity_identifier = '{}-{}-{}'.format(evidence.entity_id, evidence.table, evidence.row_id)
#
#                 if evidence_per_entity_identifier not in evidence_per_entity_identifiers[entity_type]:
#                     evidence_per_entity = {'query_table_id': evidence.query_table_id, 'entity_id': evidence.entity_id,
#                                            'table': evidence.table, 'row_id': evidence.row_id}
#                     evidence_per_entity_identifiers[entity_type][evidence_per_entity_identifier] = evidence_per_entity
#
#         for query_table in query_tables:
#             entity_type = 'head' if 'head' in query_table.assembling_strategy else 'tail'
#
#             evidence_id = max([evidence.identifier for evidence in query_table.verified_evidences]) + 1
#
#             for evidence_per_entity_identifier in evidence_per_entity_identifiers[entity_type]:
#                 evidence_found = False
#                 for evidence in query_table.verified_evidences:
#                     evidence_per_entity_identifier_existing = '{}-{}-{}'.format(evidence.entity_id, evidence.table,
#                                                                                 evidence.row_id)
#                     if evidence_per_entity_identifier == evidence_per_entity_identifier_existing:
#                         evidence_found = True
#                         break
#
#                 if not evidence_found:
#                     entity_id = evidence_per_entity_identifiers[entity_type][evidence_per_entity_identifier][
#                         'entity_id']
#                     table_name = evidence_per_entity_identifiers[entity_type][evidence_per_entity_identifier]['table']
#                     row_id = evidence_per_entity_identifiers[entity_type][evidence_per_entity_identifier]['row_id']
#                     evidence = Evidence(evidence_id, query_table.identifier, entity_id, None,
#                                         table_name, row_id, query_table.target_attribute, None)
#                     evidence.signal = True
#                     evidence.scale = 1
#                     evidence_id += 1
#                     query_table.add_verified_evidence(evidence)
#
#         # # Remove rows with 0 evidences
#         for query_table in query_tables:
#             evidences_no_ground_truth = strategy_obj.filter_evidences_by_ground_truth_tables(
#                 query_table.verified_evidences)
#             total_rows += len(query_table.table)
#             for row in query_table.table:
#                 relevant_evidences = [evidence for evidence in evidences_no_ground_truth
#                                         if evidence.entity_id == row['entityId']]
#
#                 # Remove with 0 evidences --> out of corpus entities
#                 # Remove with 4 evidences, because tail entities are defined from 1-3 and head entities 5-n
#                 if len(relevant_evidences) == 0:
#                     if query_table.assembling_strategy not in ooc_entities_by_assembling_strategy:
#                         ooc_entities_by_assembling_strategy[query_table.assembling_strategy] = set()
#                     ooc_entities_by_assembling_strategy[query_table.assembling_strategy].add(row['entityId'])
#
#                 elif 'head' in query_table.assembling_strategy:
#                     if len(relevant_evidences) < 5:
#                         if query_table.assembling_strategy not in ooc_entities_by_assembling_strategy:
#                             ooc_entities_by_assembling_strategy[query_table.assembling_strategy] = set()
#                         ooc_entities_by_assembling_strategy[query_table.assembling_strategy].add(row['entityId'])
#
#                         if len(relevant_evidences) < 4:
#                             # Make head entity to tail entity
#                             tail_entity = {'row': row, 'evidences': relevant_evidences}
#                             new_usecase_strategy = query_table.use_case.replace('head', 'tail')
#                             if new_usecase_strategy not in wrongly_assigned_entities:
#                                 wrongly_assigned_entities[new_usecase_strategy] = []
#                             wrongly_assigned_entities[new_usecase_strategy].append(tail_entity)
#
#                 elif 'tail' in query_table.assembling_strategy:
#                     if len(relevant_evidences) > 3:
#                         if query_table.assembling_strategy not in ooc_entities_by_assembling_strategy:
#                             ooc_entities_by_assembling_strategy[query_table.assembling_strategy] = set()
#                         ooc_entities_by_assembling_strategy[query_table.assembling_strategy].add(row['entityId'])
#
#                         if len(relevant_evidences) > 4:
#                             # Make tail entity to head entity
#                             head_entity = {'row': row, 'evidences': relevant_evidences}
#                             new_usecase_strategy = query_table.use_case.replace('tail', 'head')
#                             if new_usecase_strategy not in wrongly_assigned_entities:
#                                 wrongly_assigned_entities[new_usecase_strategy] = []
#                             wrongly_assigned_entities[new_usecase_strategy].append(head_entity)
#
#         for query_table in query_tables:
#             #     # Remove rows without evidences
#             if query_table.assembling_strategy in ooc_entities_by_assembling_strategy:
#                 removable_entities = ooc_entities_by_assembling_strategy[query_table.assembling_strategy]
#                 query_table.table = [row for row in query_table.table if row['entityId'] not in removable_entities]
#                 query_table.verified_evidences = [evidence for evidence in query_table.verified_evidences
#                                                   if evidence.entity_id not in removable_entities]
#
#             # Add wrongly assigned head entities to tail entities
#             new_entity_id = 100
#             if query_table.use_case in wrongly_assigned_entities:
#                 for entity in wrongly_assigned_entities[query_table.use_case]:
#                     row = entity['row'].copy()
#                     row['entityId'] = new_entity_id
#                     query_table.table.append(row)
#                     for evidence in entity['evidences']:
#                         new_evidence = evidence.__copy__()
#                         new_evidence.query_table_id = query_table.identifier
#                         new_evidence.entity_id = entity_id
#                         query_table.add_verified_evidence(new_evidence)
#                     new_entity_id += 1
#
#
#             query_table.normalize_query_table_numbering()
#             query_table.save(with_evidence_context=False)
#
#     logger.info('Total number of rows: ' + str(total_rows))
#     logger.info('Reduced number of rows: ' + str(reduced_rows))

def load_and_save_querytables():
    schema_org = 'localbusiness'

    categories = get_gt_tables('retrieval', schema_org)
    #categories = ['accor_com']
    strategy_obj = QueryByEntity(schema_org)

    for category in categories:
        query_table_paths = get_query_table_paths('retrieval', schema_org, category)
        query_tables = [load_query_table_from_file(path) for path in query_table_paths]

        for query_table in query_tables:
            query_table.save(with_evidence_context=False)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger()

    # Load environmental parameters
    path_to_data = os.environ['DATA_DIR']

    load_and_save_querytables()
    #query_table_goldstandard = load_and_correct_goldstandard()

    logger.info('Corrected query tables!')
