import json
import logging
import os

import click
from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.strategy.open_book.es_helper import determine_es_index_name

@click.command()
@click.option('--cluster_size', type=int)
def calculate_cluster_statistics(cluster_size):
    logger = logging.getLogger()

    es_index = determine_es_index_name('localbusiness')
    elastic_instance = os.environ['ES_INSTANCE']
    _es = Elasticsearch([{'host': elastic_instance, 'port': 9200}])

    path_to_cluster = '{}/cluster/localbusiness/telephone_geo_cluster_summary.json'.format(os.environ['DATA_DIR'])
    clusters = load_cluster(path_to_cluster)

    cluster_ids = []
    unique_tables = set()
    for cluster in tqdm(clusters):
        if cluster['size'] > cluster_size:
            for record in cluster['records']:
                cluster_ids.append(record)
                if len(cluster_ids) > 999:
                    query_body = {
                                'size': len(cluster_ids),
                                'query': {
                                    'bool': {'must': [
                                        {'terms': {'_id': [str(identifier) for identifier in cluster_ids]}}
                                    ]
                                    }
                                }
                            }

                    # Retrieve from cluster
                    result = _es.search(body=json.dumps(query_body), index=es_index)
                    for hit in result['hits']['hits']:
                        unique_tables.add(hit['_source']['table'])
                    cluster_ids = []

    logger.info('Number of unique tables covered by clusters with size {} and greater: {}'.format(
        cluster_size, len(list(unique_tables))))

def load_cluster(path_to_cluster):
    clusters = []
    cluster_id = 0

    # Load Telephone Cluster
    with open(path_to_cluster, 'r') as f:
        lines = f.readlines()
        # Strips the newline character
        for line in lines:
            cluster = json.loads(line)
            cluster['records'] = cluster['records'].split(', ')
            cluster['size'] = int(cluster['size'])
            clusters.append(cluster)

    return clusters

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    calculate_cluster_statistics()