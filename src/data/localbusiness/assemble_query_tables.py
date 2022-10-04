import json
import logging
import os

import click
from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.data.localbusiness.load_clusters import load_clusters
from src.model.evidence import Evidence
from src.model.evidence_new import RetrievalEvidence
from src.model.querytable_new import RetrievalQueryTable
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity


def determine_cluster_hits(records, schema_org_class):
    query_strategy = QueryByEntity(schema_org_class)
    index_name = determine_es_index_name(schema_org_class, clusters=True)
    ids = [record[0] for record in records]
    cluster_hits = []
    for record in records:
        hit = query_strategy.query_tables_index_by_table_row_id(record[1], record[2], index_name)
        cluster_hits.append(hit)

    return cluster_hits


def determine_ooc_hits(excluded_ids, schema_org_class, table, country, all_attributes):
    _es = Elasticsearch([{'host': os.environ['ES_INSTANCE'], 'port': 9200}])
    query_body = {
        'size': 1000,
        'query': {
            'bool': {
                'should': [
                    {'match': {'table': {'query': table}}}
                ],
                'must_not': [
                    {'terms': {'_id': [str(identifier) for identifier in list(excluded_ids)[:65000]]}}
                ]
            }
        }
    }

    # Retrieve entities with phone numbers
    es_index = determine_es_index_name(schema_org_class)
    result = []
    for hit in _es.search(body=json.dumps(query_body), index=es_index)['hits']['hits']:
        if 'table' in hit['_source'] and table == hit['_source']['table'] \
                and 'addressregion' in hit['_source'] and hit['_source']['addressregion'] in country \
                and all([attribute in hit['_source'] for attribute in all_attributes]):
            result.append(hit)

    return result


def collect_entities(collected_phone_numbers, entities, clusters, schema_org_class, table, country, all_attributes):
    """Collect entities"""
    head_ids = []
    tail_ids = []
    for cluster in tqdm(clusters):
        if table in [record[1] for record in cluster['records']]:
            if 5 < cluster['size'] <= 20 and cluster['no_tables'] > 3:
                head_ids.append(cluster['records'])
            elif cluster['size'] == 3 and cluster['no_tables'] > 2:
                tail_ids.append(cluster['records'])

    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    # Collect head ids
    n = 1000
    for cluster_records in head_ids:
        hits = determine_cluster_hits(cluster_records, schema_org_class)
        different_values = set()
        for hit in hits:
            different_values.add(hit['name'])

        if len(different_values) < 5:
            continue
        for hit in hits:
            if hit['table'] == table \
                    and 'telephone' in hit \
                    and hit['telephone'] not in collected_phone_numbers \
                    and 'addresscountry' in hit \
                    and all([attribute in hit for attribute in all_attributes]):
                collected_phone_numbers.append(hit['telephone'])
                if len(entities['head']) < 30:
                    hit['id'] = hit['_id']
                    entities['head'].append(hit)
                else:
                    break
        if len(entities['head']) == 30:
            break

    # Collect tail ids
    for cluster_records in tail_ids:
        hits = determine_cluster_hits(cluster_records, schema_org_class)
        different_values = set()
        for hit in hits:
            different_values.add(hit['name'])

        if len(different_values) < 2:
            continue
        for hit in hits:
            if hit['table'] == table \
                    and 'telephone' in hit \
                    and hit['telephone'] not in collected_phone_numbers \
                    and 'addresscountry' in hit \
                    and all([attribute in hit for attribute in all_attributes]):
                collected_phone_numbers.append(hit['telephone'])
                if len(entities['tail']) < 30:
                    hit['id'] = hit['_id']
                    entities['tail'].append(hit)
                else:
                    break

        if len(entities['tail']) == 30:
            break


def collect_ooc_entities(collected_phone_numbers, entities, all_clusters, schema_org_class, table, country,
                         all_attributes):
    ids_cluster_entities = set()
    for cluster in all_clusters:
        ids_cluster_entities.update(cluster['records'])

    hits = determine_ooc_hits(ids_cluster_entities, schema_org_class, table, country, all_attributes)
    for hit in hits:
        if 'telephone' in hit['_source'] and hit['_source']['telephone'] not in collected_phone_numbers:
            collected_phone_numbers.append(hit['_source']['telephone'])
            if len(entities['tail']) < 30:
                hit['_source']['id'] = hit['_id']
                entities['tail'].append(hit['_source'])
            else:
                break


@click.command()
@click.option('--table')
@click.option('--country')
@click.option('--schema_org_class')
@click.option('--initial_qt_id', type=int)
def assemble_query_tables(table, country, schema_org_class, initial_qt_id):
    """Assembles two query tables based on the provided table: 1. Head Entities 2. Tail Entities"""

    tel_geo_clusters = load_clusters('localbusiness/telephone_geo_cluster_summary.json')
    #tel_clusters = load_clusters('localbusiness/telephone_cluster_summary.json')

    tables = set()
    for cluster in tel_geo_clusters:
        for record in cluster['records']:
            tables.add(record[1])

    print('{} tables in index'.format(len(tables)))

    # Filter clusters for clusters with requested table
    tel_geo_clusters = [cluster for cluster in tel_geo_clusters if table in cluster['tables']]
    #tel_clusters = [cluster for cluster in tel_clusters if table in cluster['tables']]

    all_clusters = tel_geo_clusters.copy()
    #all_clusters.extend(tel_clusters)

    # Collect head and tail entities for query tables
    entities = {'head': [], 'tail': []}
    collected_phone_numbers = []

    # Necessary Attributes
    target_attributes = ['addresslocality', 'addressregion', 'addresscountry', 'postalcode', 'streetaddress',
                         'telephone']
    context_attributes = ['addresslocality', 'addressregion', 'addresscountry', 'postalcode', 'name', 'streetaddress']
    #target_attributes = ['addresslocality', 'addressregion', 'postalcode', 'streetaddress',
    #                     'telephone']
    #context_attributes = ['addresslocality', 'addressregion', 'postalcode', 'name', 'streetaddress']
    #country = ['NL', 'ES', 'FR', 'DE', 'GB', 'IT', 'AT', 'CH', 'CE', 'GR']
    country = ['United Kingdom']

    all_attributes = set()
    all_attributes.update(target_attributes)
    all_attributes.update(context_attributes)

    collect_entities(collected_phone_numbers, entities, tel_geo_clusters, schema_org_class, table, country,
                     all_attributes)
    logging.info('Collected entities from telephone and geo clusters.')

    # if len(entities['head']) < 30 or len(entities['tail']) < 30:
    #     collect_entities(collected_phone_numbers, entities, tel_clusters, schema_org_class, table, country,
    #                      all_attributes)
    #     logging.info('Collected entities from telephone clusters.')
    #
    # logging.info('Found {} tail entities!'.format(len(entities['tail'])))
    # # Find out of corpus entities
    # if len(entities['tail']) < 30:
    #     collect_ooc_entities(collected_phone_numbers, entities, all_clusters, schema_org_class, table, country,
    #                          all_attributes)

    logging.info('Found {} head entities!'.format(len(entities['head'])))
    logging.info('Found {} tail & ooc entities!'.format(len(entities['tail'])))

    # assert len(entities['head']) == 30
    # assert len(entities['tail']) == 30

    # Generate query table
    qt_id = initial_qt_id

    entity_evidences = []
    all_entities = entities['head'].copy()
    all_entities.extend(entities['tail'].copy())

    entity_id = 0
    for entity in all_entities:
        entity['entityId'] = entity_id
        entity_id += 1

        # Collect provenance information (evidence)
        evidence_attributes = ['id', 'table', 'row_id', 'entityId']
        entity_evidences.append({attribute: entity[attribute] for attribute in evidence_attributes})

        # Prepare entity for query table
        attributes_to_be_removed = []
        for attribute in entity:
            if attribute not in context_attributes \
                    and attribute not in target_attributes and attribute != 'entityId':
                attributes_to_be_removed.append(attribute)
        for attribute in attributes_to_be_removed:
            del entity[attribute]


    category = table.split('_')[1]
    assembling_strategy = 'Collection of entities from {}'.format(category)
    category = ' '.join([value.capitalize() for value in category.split('.')])

    # Filter attributes
    filtered_entities = []
    for entity in all_entities:
        reduced_entity = entity.copy()
        removal_attributes = [attribute for attribute in reduced_entity.keys()
                              if attribute not in context_attributes
                              and attribute != 'entityId']
        for attribute in removal_attributes:
            reduced_entity.pop(attribute)
        filtered_entities.append(reduced_entity)

    verified_evidences = []
    evidence_id = 0
    for entity in all_entities:
        raw_evidence = entity_evidences[entity['entityId']]
        original_identifier = '{}-{}'.format(raw_evidence['table'], raw_evidence['row_id'])
        # First evidence --> Selected table, which will be removed from Table Corpus later on
        evidence = RetrievalEvidence(evidence_id, qt_id, raw_evidence['entityId'],
                            raw_evidence['table'], raw_evidence['row_id'], None)
        evidence.signal = True
        evidence.scale = 3
        verified_evidences.append(evidence)
        evidence_id += 1

        # Find evidences from cluster
        found_cluster = None
        # for cluster in tel_clusters:
        #     if original_entity_id in cluster['records']:
        #         found_cluster = cluster
        #         break
        for cluster in tel_geo_clusters:

            if original_identifier in ['{}-{}'.format(record[1], record[2]) for record in cluster['records']]:
                found_cluster = cluster
                break
        if found_cluster is None:
            logging.warning('No cluster found for entity {}!'.format(original_identifier))

        else:
            hits = determine_cluster_hits(found_cluster['records'], schema_org_class)
            for hit in hits:
                if hit['table'] != table:
                    evidence = RetrievalEvidence(evidence_id, qt_id, raw_evidence['entityId'],
                                        hit['table'],
                                        hit['row_id'],
                                        None)
                    evidence.signal = True
                    #evidence.determine_scale(filtered_entities)
                    verified_evidences.append(evidence)
                    evidence_id += 1

    query_table = RetrievalQueryTable(qt_id, 'retrieval', assembling_strategy,
                             category, schema_org_class,
                             context_attributes,
                             filtered_entities, verified_evidences)
    query_table.save(with_evidence_context=False)
    qt_id += 1


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    assemble_query_tables()
