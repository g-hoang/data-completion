import json
import logging
import os
from multiprocessing import Pool

from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.strategy.open_book.es_helper import determine_es_index_name


def calculate_telephone_cluster():
    """Create cluster of localbusinesses"""
    logger = logging.getLogger()

    es_index = determine_es_index_name('localbusiness')
    elastic_instance = os.environ['ES_INSTANCE']
    _es = Elasticsearch([{'host': elastic_instance, 'port': 9200}])

    no_entities = int(_es.cat.count(es_index, params={"format": "json"})[0]['count'])
    step_size = 1000
    #no_entities = 500

    phone_number_clusters = {}
    tables_of_clusters = {}

    for i in tqdm(range(int(no_entities / step_size) + 1)):

        start = i * step_size
        end = start + step_size
        if end > no_entities:
            end = no_entities

        # Filter ids by already found ids
        ids = [identifier for identifier in range(start, end)]
        if len(ids) > 0:
            logger.info('Will look for {} entities by id'.format(len(ids)))
            query_body = {
                'size': len(ids),
                'query': {
                    'bool': {'must': [
                        {'exists': {'field': 'telephone'}},
                        {'terms': {'_id': [str(identifier) for identifier in ids]}}
                    ]
                    }
                }
            }

            # Retrieve entities with phone numbers
            result = _es.search(body=json.dumps(query_body), index=es_index)

            unique_phone_numbers = set([hit['_source']['telephone'] for hit in result['hits']['hits']])

            for phone_number in unique_phone_numbers:

                if phone_number[:3] not in phone_number_clusters:
                    phone_number_clusters[phone_number[:3]] = {}
                    tables_of_clusters[phone_number[:3]] = {}

                if phone_number not in phone_number_clusters[phone_number[:3]]:
                    phone_number_clusters[phone_number[:3]][phone_number] = []
                    tables_of_clusters[phone_number[:3]][phone_number] = set()

                phone_number_clusters[phone_number[:3]][phone_number].extend([(hit['_id'],
                                                                               hit['_source']['table'],
                                                                               hit['_source']['row_id'])
                                                                              for hit in result['hits']['hits']
                                                                              if hit['_source'][
                                                                                  'telephone'] == phone_number])
                tables_of_clusters[phone_number[:3]][phone_number].update([hit['_source']['table']
                                                                           for hit in result['hits']['hits']
                                                                           if hit['_source']['telephone'] == phone_number])

    aggregate_telephone_cluster(phone_number_clusters, tables_of_clusters)


def aggregate_telephone_cluster(phone_number_clusters, tables_of_clusters):
    logging.info('Aggregate Telephone Clusters')

    cluster_summary_file_path = '{}/cluster/localbusiness/telephone_cluster_summary.json'.format(os.environ['DATA_DIR'])
    open(cluster_summary_file_path, 'w').close()

    for first_digits in phone_number_clusters.keys():
        for telephone_number in phone_number_clusters[first_digits].keys():
            size = len(phone_number_clusters[first_digits][telephone_number])
            if size > 1:

                ids = phone_number_clusters[first_digits][telephone_number]
                tables = list(tables_of_clusters[first_digits][telephone_number])
                no_tables = len(list(tables_of_clusters[first_digits][telephone_number]))
                cluster = {'telephone_number': telephone_number, 'records': ids, 'size': size,
                           'tables': tables, 'no_tables': no_tables}

                with open(cluster_summary_file_path, 'a') as csf:
                    csf.write(json.dumps(cluster) + '\n')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    calculate_telephone_cluster()
