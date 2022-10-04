import os
import logging

import fasttext

from src.model.evidence import Evidence
from src.model.querytable import load_query_tables, load_query_tables_by_class, get_categories, get_query_table_paths, \
    load_query_table_from_file, QueryTable
from src.preprocessing.language_detection import LanguageDetector
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity


def split_query_tables_by_corner_cases():
    logger = logging.getLogger()

    # Load query tables
    schema_org = 'localbusiness'

    categories = get_categories(schema_org)
    #categories = ['michelin_com']

    for category in categories:
        query_table_paths = get_query_table_paths(schema_org, category)
        query_tables = [load_query_table_from_file(path) for path in query_table_paths]

        for query_table in query_tables:

            # Construct cc query table
            cc_identifier = query_table.identifier + 300
            cc_assembling_strategy = query_table.assembling_strategy.replace('entities', 'cc entities')
            cc_use_case = query_table.use_case.replace('entities', 'cc entities')
            cc_requirements = query_table.requirements
            cc_context_attributes = query_table.context_attributes
            cc_target_attribute = query_table.target_attribute

            # Split actual table with verified evidences
            cc_table =[]
            cc_verified_evidences = []

            non_cc_table = []
            non_cc_verified_evidences = []

            # Drop duplicated evidences
            query_table.verified_evidences = list(set(query_table.verified_evidences))

            for row in query_table.table:
                found_evidences = [evidence for evidence in query_table.verified_evidences
                                   if evidence.entity_id == row['entityId']]
                found_corner_cases = [evidence for evidence in query_table.verified_evidences
                                        if evidence.entity_id == row['entityId'] and evidence.corner_case]
                if len(found_evidences) == 0:
                    # Out-of-corpus entity
                    continue
                elif len(found_corner_cases) / len(found_evidences) > 0.5:
                    cc_table.append(row)
                    for evidence in found_evidences:
                        evidence.query_table_id = cc_identifier
                        cc_verified_evidences.append(evidence)

                else:
                    non_cc_table.append(row)
                    non_cc_verified_evidences.extend(found_evidences)

            if len(non_cc_table) > 2:
                query_table.table = non_cc_table
                query_table.verified_evidences = non_cc_verified_evidences

                query_table.normalize_query_table_numbering()
                query_table.save(with_evidence_context=False)
            else:
                # Delete old query table
                category = query_table.gt_table.lower().replace(" ", "_")
                file_name = 'gs_querytable_{}_{}_{}.json'.format(category, query_table.target_attribute,
                                                                 query_table.identifier)
                path_to_query_table = '{}/querytables/{}/{}/{}'.format(os.environ['DATA_DIR'],
                                                                       query_table.schema_org_class,
                                                                       category, file_name)
                #if os.path.exists(path_to_query_table):
                os.remove(path_to_query_table)


            if len(cc_table) > 2:
                cc_query_table = QueryTable(cc_identifier, cc_assembling_strategy, cc_use_case, query_table.gt_table,
                                            query_table.schema_org_class, cc_requirements, cc_context_attributes,
                                            cc_target_attribute, cc_table, cc_verified_evidences)
                cc_query_table.normalize_query_table_numbering()
                cc_query_table.save(with_evidence_context=False)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger()

    split_query_tables_by_corner_cases()

    logger.info('Query Tables split!')
