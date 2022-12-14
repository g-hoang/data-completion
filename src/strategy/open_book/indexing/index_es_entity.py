import gzip
import json
import os
import time

import click
from elasticsearch import Elasticsearch, helpers
import logging

from multiprocessing import Pool

from tqdm import tqdm

from src.data.localbusiness.load_clusters import load_clusters as load_localbusiness_clusters
from src.data.product.load_clusters import load_clusters as load_product_clusters
from src.preprocessing.entity_extraction import extract_entity
from src.strategy.open_book.es_helper import determine_es_index_name
from src.preprocessing.language_detection import LanguageDetector


@click.command()
@click.option('--schema_org_class')
@click.option('--worker', help='Number of workers', type=int)
@click.option('--tokenizer', help='Tokenizer for ES Index', default='standard')
@click.option('--no-test/--test', default=True)
@click.option('--with-clusters/--without-clusters', default=False)
def load_data(schema_org_class, worker, tokenizer, no_test, with_clusters):
    logger = logging.getLogger()

    # Connect to Elasticsearch
    _es = Elasticsearch([{'host': os.environ['ES_INSTANCE'], 'port': 9200}], timeout=15, max_retries=3, retry_on_timeout=True)

    if not _es.ping():
        raise ValueError("Connection failed")

    clusters = None
    if with_clusters:
        # To-Do: Normalize paths to clusters
        if schema_org_class == 'localbusiness':
            path_to_cluster = 'localbusiness/telephone_geo_cluster_summary.json'
            raw_clusters = load_localbusiness_clusters(path_to_cluster)
        elif schema_org_class == 'product':
            path_to_lspc_table_corpus_mappings = '{}/cluster/product/lspc2020_to_tablecorpus'.format(
                os.environ['DATA_DIR'])
            raw_clusters = load_product_clusters(
                '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'filtered_product_clusters.json.gz'), None)
        else:
            ValueError('Schema Org class {} is not known'.format(schema_org_class))

        clusters = {}
        no_clustered_records = 0
        for cluster in tqdm(raw_clusters):
            for record in cluster['records']:
                # To-Do: Normalize structure of clusters
                if schema_org_class == 'product':
                    table = record['table_id'].lower().split('_')[1]
                elif schema_org_class == 'localbusiness':
                    table = record[1].lower().split('_')[1]
                else:
                    raise ValueError('Schema Org class {} is unknown'.format(schema_org_class))
                if table[:3] not in clusters:
                    clusters[table[:3]] = {}

                if table not in clusters[table[:3]]:
                    clusters[table[:3]][table] = {}

                if schema_org_class == 'product':
                    row_id = record['row_id']
                elif schema_org_class == 'localbusiness':
                    row_id = record[2]
                if row_id not in clusters[table[:3]][table]:
                    clusters[table[:3]][table][row_id] = cluster['cluster_id']
                    no_clustered_records += 1

    if with_clusters:
        logger.info('Found {} clustered records'.format(no_clustered_records))
    # Load data into one index:
    #  1. Index with a fixed schema (Schema is based on Schema.org)
    entity_index = 0
    if tokenizer == 'standard':
        mapping = '''{"mappings": {
                    "date_detection": false
                    }}'''
        entity_index_name = determine_es_index_name(schema_org_class, clusters=with_clusters)
    elif tokenizer == 'n-gram':
        # Create index with 2-gram tokenization
        mapping = '''{"mappings": {
                    "date_detection": false
                    },
                    "settings": {
                        "analysis": {
                          "analyzer": {
                            "my_analyzer": {
                              "tokenizer": "ngram_tokenizer"
                            }
                          },
                          "tokenizer": {
                            "ngram_tokenizer": {
                              "type": "ngram",
                              "min_gram": 1,
                              "max_gram": 2,
                              "token_chars": [
                                "letter",
                                "digit"
                              ]
                            }
                          }
                        }
                      }}'''
        entity_index_name = determine_es_index_name(schema_org_class, tokenizer='n-gram', clusters=with_clusters)

    time.sleep(5)
    if no_test:
        _es.indices.delete(entity_index_name, ignore=[404])

        _es.indices.create(entity_index_name, body=mapping)

    # Collect statistics about added/ not added entities & tables
    index_statistics = {'tables_added': 0, 'tables_not_added': 0, 'entities_added': 0, 'entities_not_added': 0}

    directory = '{}/corpus/{}'.format(os.environ['DATA_DIR'], schema_org_class)

    # Prepare parallel processing
    results = []
    if worker > 0:
        pool = Pool(worker)
    collected_filenames = []

    for filename in os.listdir(directory):

        if clusters is not None:
            #Check if table connected to file has clustered records
            file_table = filename.lower().split('_')[1]
            if file_table[:3] not in clusters:
                continue
            if file_table not in clusters[file_table[:3]]:
                continue

        collected_filenames.append(filename)
        if len(collected_filenames) > 50:
            if worker == 0:
                results.append(create_table_index_action(directory, collected_filenames, entity_index_name, schema_org_class, clusters))
            else:
                results.append(
                    pool.apply_async(create_table_index_action, (directory, collected_filenames, entity_index_name,
                                                                 schema_org_class, clusters,)))
            collected_filenames = []

    if len(collected_filenames) > 0:
        if worker == 0:
            results.append(create_table_index_action(directory, collected_filenames, entity_index_name, schema_org_class, clusters))
        else:
            results.append(
                 pool.apply_async(create_table_index_action, (directory, collected_filenames, entity_index_name,
                                                              schema_org_class, clusters)))

    pbar = tqdm(total=len(results))
    logger.info('Wait for all tasks to finish!')

    while True:
        if len(results) == 0:
            break

        results, entity_index = send_actions_to_elastic(_es, results, entity_index, index_statistics, pbar, no_test, worker)

    pbar.close()

    if worker > 0:
        pool.close()
        pool.join()

    # Report statistics about indexing
    logger.info('Added entities: {}'.format(index_statistics['entities_added']))
    logger.info('Not added entities: {}'.format(index_statistics['entities_not_added']))
    logger.info('Added tables: {}'.format(index_statistics['tables_added']))
    logger.info('Not added tables: {}'.format(index_statistics['tables_not_added']))


def send_actions_to_elastic(_es, results, entity_index, index_statistics, pbar, no_test, worker):
    """Send actions to elastic and update statistics"""
    logger = logging.getLogger()

    collected_results = []
    actions = []

    for result in results:
        new_actions, new_statistics = None, None
        if worker == 0:
            new_actions, new_statistics = result
        elif result.ready():
            new_actions, new_statistics = result.get()

        if new_actions is not None and new_statistics is not None:
            logger.debug('Retrieved {} actions'.format(len(new_actions)))
            for action in new_actions:
                action['_id'] = entity_index
                actions.append(action)

                entity_index += 1

            index_statistics['entities_added'] += new_statistics['entities_added']
            index_statistics['entities_not_added'] += new_statistics['entities_not_added']
            collected_results.append(result)
            pbar.update(1)

    if len(actions) > 0 and no_test:
        # Add entities to ES
        helpers.bulk(client=_es, actions=actions, chunk_size=1000, request_timeout=60)

    # Remove collected results from list of results
    results = [result for result in results if result not in collected_results]

    return results, entity_index


def create_table_index_action(directory, files, entity_index, schema_org_class, clusters):
    """Creates entity document that will be index to elastic search"""
    log_format = '%(asctime)s - subprocess - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logger = logging.getLogger()

    ld = LanguageDetector()

    actions = []
    index_statistics = {'entities_added': 0, 'entities_not_added': 0}

    for filename in files:
        file_path = '{}/{}'.format(directory, filename)

        if clusters is not None:
            #Check if table connected to file has clustered records
            file_table = filename.lower().split('_')[1]
            if file_table[:3] not in clusters:
                continue
            if file_table not in clusters[file_table[:3]]:
                continue
        else:
            file_table = None

        # Use 'entity' to find entity indices
        found_entities = []
        try:
            with gzip.open(file_path, 'rb') as file:
                # 1. Fill index with normalized entities
                for line in file.readlines():
                    # Extract entity name
                    raw_entity = json.loads(line)
                    if clusters is not None and file_table is not None:
                        if raw_entity['row_id'] not in clusters[file_table[:3]][file_table]:
                            index_statistics['entities_not_added'] += 1
                            continue

                    if 'name' in raw_entity \
                            and raw_entity['name'] is not None \
                            and len(raw_entity['name']) > 0:
                        # Detect language of entity
                        if ld.check_language_is_not_english(raw_entity['name']):
                            logger.debug('TABLE INDEX ERROR - Language of entity {} is not english.'
                                         .format(raw_entity['name']))
                            index_statistics['entities_not_added'] += 1

                        else:
                            entity = extract_entity(raw_entity, schema_org_class)
                            # Determine duplicates based on entity values without description
                            entity_wo_description = entity.copy()
                            if 'description' in entity_wo_description:
                                del entity_wo_description['description']

                            if 'name' in entity \
                                    and len(entity.keys()) > 1 \
                                    and entity_wo_description not in found_entities:

                                found_entities.append(entity_wo_description)

                                entity['table'] = filename.lower()
                                entity['row_id'] = raw_entity['row_id']
                                entity['page_url'] = raw_entity['page_url']

                                actions.append({'_index': entity_index, '_source': entity})
                                index_statistics['entities_added'] += 1
                            else:
                                index_statistics['entities_not_added'] += 1

                    else:
                        logger.debug(
                            'TABLE INDEX ERROR - Entity does not have a name attribute: {} - not added to index: {}'
                                .format(str(raw_entity), filename))
                        index_statistics['entities_not_added'] += 1

        except gzip.BadGzipFile as e:
            logger.warning('{} - Cannot open file {}'.format(e, filename))

    logger.debug('Added {} actions'.format(index_statistics['entities_added']))

    return actions, index_statistics


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    load_data()
