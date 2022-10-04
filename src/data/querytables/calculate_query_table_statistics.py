import json
import os
import logging

from elasticsearch import Elasticsearch

from src.model.querytable import load_query_tables


def calculate_statistics():
    logger = logging.getLogger()

    # Load query tables
    no_scale_3_count = 0
    counter_entity_frequencies = {}
    querytables = load_query_tables()
    corner_cases = 0

    for querytable in querytables:
        #querytable.calculateNOevidencesByScale()
        # Find rows without scale 3 annotations
        for row in querytable.table:
            count_matching_entities = 0
            not_found = True
            for evidence in querytable.verified_evidences:
                if evidence.entity_id == row['entityId']:
                    if evidence.scale == 3 or evidence.scale == 2 or evidence.scale == 1:
                        if evidence.scale == 3:
                            logger.info('Scale 3 evidence found!')
                            not_found = False

                        count_matching_entities += 1
                        context = retrieve_evidence_context(evidence)
                        if context is not None and type(context['name']) is str and context['name'] != row['name']:
                            corner_cases += 1

            # Target Attribute Value not found
            if not_found:
                no_scale_3_count += 1

            if count_matching_entities in counter_entity_frequencies:
                counter_entity_frequencies[count_matching_entities] += 1
            else:
                counter_entity_frequencies[count_matching_entities] = 1


    logger.info('No. rows without scale 3 evidence: ' + str(no_scale_3_count))

    logger.info('------------------')
    head_tail_ooc_counts = {'head': 0, 'tail': 0, 'ooc':0}
    for key in counter_entity_frequencies.keys():
        if key >= 20:
            head_tail_ooc_counts['head'] += counter_entity_frequencies[key]
        elif key < 3 and key > 0:
            head_tail_ooc_counts['tail'] += counter_entity_frequencies[key]
        elif key == 0:
            head_tail_ooc_counts['ooc'] += counter_entity_frequencies[key]

    logger.info('Head entities: {}'.format(head_tail_ooc_counts['head']))
    logger.info('Tail entities: {}'.format(head_tail_ooc_counts['tail']))
    logger.info('OOC entities: {}'.format(head_tail_ooc_counts['ooc']))

    logger.info('Annotated corner cases: {}'.format(corner_cases))


def retrieve_evidence_context(evidence):
    """Retrieve evidence context from ES"""
    elastic_instance = os.environ['ES_INSTANCE']
    _es = Elasticsearch([{'host': elastic_instance, 'port': 9200}])
    query_body = {
        'size': 1,
        'query':
            {
                'bool':
                    {
                     'must': [{'match': {'table': {'query': evidence.table}}},
                              {'match': {'row_id': {'query': evidence.row_id}}}]
                     }
            }
    }

    query_results = _es.search(body=json.dumps(query_body), index='normalized_entity_index_hotel_bert-base-uncased')

    context = None
    if len(query_results['hits']['hits']) > 0:
        context = query_results['hits']['hits'][0]['_source']

    return context


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger()

    # Load environmental parameters
    path_to_data = os.environ['DATA_DIR']

    calculate_statistics()

    logger.info('Statistics calculated!')
