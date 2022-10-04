import gzip
import json
import logging
import os
from collections import defaultdict
from itertools import repeat
from multiprocessing import Pool

import click
from tqdm import tqdm

from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity

@click.command()
@click.option('--worker', type=int)
def filter_product_clusters(worker):
    logger = logging.getLogger()
    schema_org_class = 'product'

    index_name = determine_es_index_name(schema_org_class)

    path_to_lspc_table_corpus_mappings = '{}/cluster/product/lspc2020_to_tablecorpus'.format(os.environ['DATA_DIR'])
    clusters = defaultdict(list)
    logger.info('Load Mappings')

    for filename in tqdm(os.listdir(path_to_lspc_table_corpus_mappings)):
        input_file_path = '{}/{}'.format(path_to_lspc_table_corpus_mappings, filename)
        try:
            with gzip.open(input_file_path, 'rb') as file:
                for line in file.readlines():
                    raw_mapping = json.loads(line)
                    clusters[raw_mapping['cluster_id']].append({'row_id': raw_mapping['row_id'], 'table_id': raw_mapping['table_id'].lower()})

        except gzip.BadGzipFile as e:
            logger.warning('{} - Cannot open file {}'.format(e, input_file_path))

    logger.info('Loaded all clusters!')
    filtered_clusters = defaultdict(list)
    found_tables_cluster = set()
    for cluster_id, records in clusters.items():
        # Exclude clusters for which only record is found
        if len(records) > 1:
            # Check if records do not come from the same source
            cluster_tables = set([record['table_id'] for record in records])
            if len(cluster_tables):
                found_tables_cluster.update(cluster_tables)
                filtered_clusters[cluster_id] = records

    logger.info('Filtered Clusters by size & number of sources')
    logger.info('Found {} unique sources'.format(len(found_tables_cluster)))

    try:
        output_file_path = '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'product_clusters.json.gz')
        with gzip.open(output_file_path, 'wb') as file:
            for cluster in filtered_clusters.items():
                if cluster is not None:
                    cluster_id, found_records = cluster
                    cluster = {'cluster_id': cluster_id, 'records': found_records}
                    file.write('{}\n'.format(json.dumps(cluster)).encode())

    except gzip.BadGzipFile as e:
        logger.warning('{} - Cannot open file {}'.format(e, output_file_path))

    #
    # pool = Pool(worker)
    # found_tables_index = pool.starmap(query_tables_index_by_table_id, zip(list(found_tables_cluster), repeat(index_name)))
    # found_tables_index = [table_id for table_id in found_tables_index if table_id is not None]
    #
    # logger.info('Checked index tables')
    # logger.info('Found {} unique sources'.format(len(found_tables_index)))
    #
    # #found_records_in_clusters = []
    # #for item in filtered_clusters.items():
    # #    found_records_in_clusters.append(find_records_in_clusters(item, found_tables_index, index_name))
    # found_records_in_clusters = pool.starmap(find_records_in_clusters, zip(filtered_clusters.items(), repeat(found_tables_index),  repeat(index_name)))
    #
    # logger.info('Filtered clusters')
    # logger.info('Found {} unique clusters'.format(len(found_records_in_clusters)))
    #
    # try:
    #     output_file_path = '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'filtered_product_clusters.json.gz')
    #     with gzip.open(output_file_path, 'wb') as file:
    #         for cluster in found_records_in_clusters:
    #             if cluster is not None:
    #                 cluster_id, found_records = cluster
    #                 cluster = {'cluster_id': cluster_id, 'records': found_records}
    #                 file.write('{}\n'.format(json.dumps(cluster)).encode())
    #
    # except gzip.BadGzipFile as e:
    #     logger.warning('{} - Cannot open file {}'.format(e, output_file_path))


def find_records_in_clusters(item, found_tables_index, index_name):
    cluster_id, records = item
    schema_org_class = 'product'
    query_strategy = QueryByEntity(schema_org_class)

    filtered_records = [record for record in records if record['table_id'] in found_tables_index]
    if len(filtered_records) > 1:
        # Check if records do not come from the same source
        if len(set([record['table_id'] for record in filtered_records])) > 1:
            found_records = []
            while len(filtered_records) > 0:
                record = filtered_records.pop(0)
                if len(filtered_records) > 0 or (filtered_records == 0 and len(found_records) > 0):
                    hit = query_strategy.query_tables_index_by_table_row_id(record['table_id'], record['row_id'],
                                                                            index_name)
                    if hit is not None:
                        found_records.append([hit['_id'], record['table_id'], record['row_id']])

            if len(found_records) > 1:
                # Check if records do not come from the same source
                if len(set([record[1] for record in found_records])) > 1:
                    return cluster_id, found_records


def query_tables_index_by_table_id(table_id, index_name):
    schema_org_class = 'product'
    query_strategy = QueryByEntity(schema_org_class)
    hit = query_strategy.query_tables_index_by_table_id(table_id, index_name)
    if hit is not None:
        return hit['table']


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    filter_product_clusters()