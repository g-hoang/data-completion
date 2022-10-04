import json
import os
import logging

import click
import fasttext

from src.model.querytable_new import load_query_tables, load_query_tables_by_class

@click.command()
@click.option('--schema_org')
def aggregate_query_tables(schema_org):
    logger = logging.getLogger()

    # Load query tables
    count = 0
    querytables = load_query_tables_by_class('retrieval', schema_org)
    path_to_aggregated_query_tables = '{}/querytables/aggregated_query_tables_{}.json'.format(os.environ['DATA_DIR'], schema_org)

    with open(path_to_aggregated_query_tables, 'w', encoding='utf-8') as f:
        for querytable in querytables:
            for row in querytable.table:
                aggregated_entry = row
                #aggregated_entry['use_case'] = querytable.use_case
                aggregated_entry['assembling_strategy'] = querytable.assembling_strategy
                #aggregated_entry['target_attribute'] = querytable.target_attribute
                aggregated_entry['query_table_id'] = querytable.identifier
                found_evidences = list(filter(lambda evidence: evidence.entity_id == row['entityId'] and evidence.signal in [1,2,3],
                                              querytable.verified_evidences))
                found_corner_cases = list(filter(lambda evidence: evidence.entity_id == row['entityId']
                                                                  and evidence.corner_case and evidence.scale in [1,2,3], querytable.verified_evidences))
                seen_training = list(filter(lambda evidence: evidence.entity_id == row['entityId']
                                                                  and evidence.seen_training, querytable.verified_evidences))
                found_matching_target_attribute_values = list(
                    filter(lambda evidence: evidence.entity_id == row['entityId'] and evidence.scale == 3,
                           querytable.verified_evidences))
                aggregated_entry['no_evidences'] = len(found_evidences)
                aggregated_entry['no_matching_target_attribute_values'] = len(found_matching_target_attribute_values)
                aggregated_entry['no_corner_cases'] = len(found_corner_cases)
                aggregated_entry['seen_during_training'] = len(seen_training)

                f.write(json.dumps(aggregated_entry) + '\n')



if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger()

    query_table_goldstandard = aggregate_query_tables()

    logger.info('Aggregated all query tables into a single goldstandard file!')
