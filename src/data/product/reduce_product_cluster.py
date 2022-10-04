import gzip
import json
import logging
import os
from collections import defaultdict
from itertools import repeat
from multiprocessing import Pool

import click
from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.data.product.load_clusters import load_clusters
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity

@click.command()
@click.option('--worker', type=int)
def reduce_product_clusters(worker):
    logger = logging.getLogger()
    schema_org_class = 'product'

    # Load cluster
    path_to_lspc_table_corpus_mappings = '{}/cluster/product/lspc2020_to_tablecorpus'.format(os.environ['DATA_DIR'])
    clusters = load_clusters('{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'product_clusters.json.gz'), None)
    # TESTING!
    #clusters = load_clusters(
    #     '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'subset_product_clusters.json'), None)

    # Prepare table/ row to cluster assignment
    table_to_row_to_cluster = {}
    for cluster in tqdm(clusters):
        for record in cluster['records']:
            table = record['table_id'].replace('product_','').replace('_september2020.json.gz', '')
            if table[:3] not in table_to_row_to_cluster:
                table_to_row_to_cluster[table[:3]] = {}

            if table not in table_to_row_to_cluster[table[:3]]:
                table_to_row_to_cluster[table[:3]][table] = {}

            if record['row_id'] not in table_to_row_to_cluster[table[:3]][table]:
                table_to_row_to_cluster[table[:3]][table][record['row_id']] = cluster['cluster_id']

    # Prepare ES connection
    strategy = QueryByEntity(schema_org_class)
    index_name = determine_es_index_name(schema_org_class)
    no_entities = int(strategy._es.cat.count(index_name, params={"format": "json"})[0]['count'])
    #no_entities = 500000

    batch_size = 1000
    final_step = int(no_entities / batch_size) + 1

    collected_clusters = {}

    logger.info('Retrieve records from ES')

    for i in tqdm(range(final_step)):
        # Determine retrieval range
        start = i * batch_size
        end = start + batch_size
        if end > no_entities:
            end = no_entities

        # Retrieve entities
        hits = strategy.query_tables_index_by_id(range(start, end), index_name)
        for hit in hits['hits']['hits']:
            table = hit['_source']['table'].replace('product_', '').replace('_september2020.json.gz', '')
            if table[:3] in table_to_row_to_cluster:
                if table in table_to_row_to_cluster[table[:3]]:
                    if hit['_source']['row_id'] in table_to_row_to_cluster[table[:3]][table]:
                        cluster_id = table_to_row_to_cluster[table[:3]][table][hit['_source']['row_id']]
                        if cluster_id not in collected_clusters:
                            collected_clusters[cluster_id] = []

                        collected_clusters[cluster_id].append({'es_id': hit['_id'], 'table_id': hit['_source']['table'],
                                                               'row_id': hit['_source']['row_id']})

    logger.info('Save Cluster!')

    try:
        output_file_path = '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'filtered_product_clusters.json.gz')
        with gzip.open(output_file_path, 'wb') as file:
            for cluster_id in tqdm(collected_clusters):
                if len(collected_clusters[cluster_id]) > 1:
                    if len(list(set([record['table_id'] for record in collected_clusters[cluster_id]]))) > 1:
                        cluster = {'cluster_id': cluster_id, 'records': collected_clusters[cluster_id]}
                        file.write('{}\n'.format(json.dumps(cluster)).encode())

    except gzip.BadGzipFile as e:
        logger.warning('{} - Cannot open file {}'.format(e, output_file_path))





if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    reduce_product_clusters()