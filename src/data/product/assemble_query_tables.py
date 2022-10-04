import json
import logging
import os

import click
from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.data.product.load_clusters import load_clusters
from src.model.evidence import Evidence
from src.model.evidence_new import RetrievalEvidence, AugmentationEvidence
from src.model.querytable import QueryTable
from src.model.querytable_new import RetrievalQueryTable, AugmentationQueryTable
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity


def determine_cluster_hits(records, schema_org_class):
    query_strategy = QueryByEntity(schema_org_class)
    index_name = determine_es_index_name(schema_org_class)
    ids = [record['es_id'] for record in records]
    hits = query_strategy.query_tables_index_by_id(ids, index_name)
    # Double-check table id & row id per record
    cluster_hits = []
    for hit in hits['hits']['hits']:
        for record in records:
            if hit['_source']['table'] == record['table_id'] \
                    and hit['_source']['row_id'] == record['row_id']:
                found_record = hit['_source']
                found_record['id'] = record['es_id']
                cluster_hits.append(found_record)

    return cluster_hits


def collect_entities(collected_cluster_ids, entities, clusters, schema_org_class, table, target_attributes, context_attributes):
    """Collect entities"""
    query_strategy = QueryByEntity(schema_org_class)

    all_attributes = set()
    all_attributes.update(target_attributes)
    all_attributes.update(context_attributes)

    concatenated_attribute_values = []

    head_clusters = []
    tail_clusters = []
    for cluster in clusters:
        # Check for clusters with a relevant amount of tables, which are not ground truth tables - retrievable
        relevant_tables = [table for table in cluster['tables'] if table not in query_strategy.ground_truth_tables]
        if 5 < len(cluster['records']) <= 20 and len(relevant_tables) > 5:
            head_clusters.append(cluster)
        elif len(cluster['records']) <= 3 and len(relevant_tables) > 0:
            tail_clusters.append(cluster)

    # Collect head entities
    for cluster in head_clusters:
        if cluster['cluster_id'] not in collected_cluster_ids:
            hits = determine_cluster_hits(cluster['records'], schema_org_class)
            if len(hits) > 5 and len(list(set([hit['table'] for hit in hits]))) > 5:
                for hit in hits:
                    if hit['table'] == table \
                            and 'name' in hit \
                            and all([attribute in hit for attribute in all_attributes]):

                        # Make sure that combination of context attributes was not collected yet
                        concatenated_attribute_value = '-'.join([hit[context_attribute]
                                                                 for context_attribute in context_attributes])
                        if cluster['cluster_id'] not in collected_cluster_ids \
                                and concatenated_attribute_value not in concatenated_attribute_values:
                            concatenated_attribute_values.append(concatenated_attribute_value)
                            collected_cluster_ids.append(cluster['cluster_id'])
                            if len(entities['head']) < 30:
                                entities['head'].append(hit)
                            else:
                                break
                        else:
                            break
                if len(entities['head']) == 30:
                    break

    # Collect tail ids
    for cluster in tail_clusters:
        if cluster['cluster_id'] not in collected_cluster_ids:
            hits = determine_cluster_hits(cluster['records'], schema_org_class)
            if len(hits) > 1 and len(list(set([hit['table'] for hit in hits]))) > 1:
                for hit in hits:
                    if hit['table'] == table \
                            and 'name' in hit \
                            and all([attribute in hit for attribute in all_attributes]):
                        if cluster['cluster_id'] not in collected_cluster_ids:
                            collected_cluster_ids.append(cluster['cluster_id'])
                            if len(entities['tail']) < 30:
                                entities['tail'].append(hit)
                            else:
                                break
                        else:
                            break
                if len(entities['tail']) == 30:
                    break


def assemble_query_tables(table, target_attributes, schema_org_class, initial_qt_id, clusters):
    """Assembles two query tables based on the provided table: 1. Head Entities 2. Tail Entities"""


    # TESTING!
    #product_clusters = load_clusters(
    #    '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'subset_filtered_product_clusters.json'), table)
    product_clusters = [cluster for cluster in clusters if table in cluster['tables']]

    collected_cluster_ids = []

    # Collect head and tail entities for query tables
    entities = {'head': [], 'tail': []}

    # Necessary Attributes
    context_attributes = ['name']

    collect_entities(collected_cluster_ids, entities, product_clusters, schema_org_class, table, target_attributes, context_attributes)
    logging.info('Collected entities from product clusters.')
    logging.info('Found {} tail entities!'.format(len(entities['tail'])))
    logging.info('Found {} head entities!'.format(len(entities['head'])))

    #assert len(entities['head']) == 30
    #assert len(entities['tail']) == 30
    #
    collected_clusters = [cluster for cluster in product_clusters if cluster['cluster_id'] in collected_cluster_ids]
    # Generate query table
    rqt_id = initial_qt_id

    # merge head & tail entities
    merged_entities = entities['head']
    merged_entities.extend(entities['tail'])

    entity_evidences = []
    entity_id = 0
    for entity in merged_entities:
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

    # Generate Retrieval Query Tables
    gt_table = table.split('_')[1]
    gt_table = ' '.join([value.capitalize() for value in gt_table.split('.')])
    assembling_strategy = 'Collection of entities from {}'.format(gt_table)

    qt_context_attributes = context_attributes.copy()

    if len(merged_entities) > 5:
        use_case = 'Retrieve entities for {}'.format(gt_table)

        # Filter attributes
        filtered_entities = []
        for entity in merged_entities:
            reduced_entity = entity.copy()
            removal_attributes = [attribute for attribute in reduced_entity.keys()
                                  if attribute not in qt_context_attributes
                                  and attribute != 'entityId']
            for attribute in removal_attributes:
                reduced_entity.pop(attribute)
            filtered_entities.append(reduced_entity)

        verified_evidences = []
        evidence_id = 0
        for entity in merged_entities:
            raw_evidence = entity_evidences[entity['entityId']]
            original_entity_id = raw_evidence['id']
            # First evidence --> Selected table, which will be removed from Table Corpus later on
            evidence = RetrievalEvidence(evidence_id, rqt_id, raw_evidence['entityId'], raw_evidence['table'],
                                         raw_evidence['row_id'], None)
            evidence.signal = True
            evidence.scale = 3
            verified_evidences.append(evidence)
            evidence_id += 1

            # Find evidences from cluster
            found_cluster = None
            for cluster in collected_clusters:
                if original_entity_id in [record['es_id'] for record in cluster['records']]:
                    found_cluster = cluster
                    break
            if found_cluster is None:
                logging.warning('No cluster found for entity {}!'.format(original_entity_id))

            else:
                hits = determine_cluster_hits(found_cluster['records'], schema_org_class)
                for hit in hits:
                    if hit['id'] != original_entity_id:
                        evidence = RetrievalEvidence(evidence_id, rqt_id, raw_evidence['entityId'],
                                                     hit['table'], hit['row_id'], None)
                        evidence.signal = True
                        verified_evidences.append(evidence)
                        evidence_id += 1

        query_table = RetrievalQueryTable(rqt_id, 'retrieval', assembling_strategy, gt_table,
                                             schema_org_class, qt_context_attributes,
                                             filtered_entities, verified_evidences)
        query_table.save(with_evidence_context=False)

    # Generate Augmentation Query Tables
    aqt_id = rqt_id + 1
    for target_attribute in target_attributes:
        qt_context_attributes = context_attributes.copy()
        if target_attribute in qt_context_attributes:
            qt_context_attributes.remove(target_attribute)

        if len(merged_entities) > 5:
            use_case = '{} of entities from {}'.format(target_attribute, gt_table)

            # Filter attributes
            filtered_entities = []
            for entity in merged_entities:
                reduced_entity = entity.copy()
                removal_attributes = [attribute for attribute in reduced_entity.keys()
                                      if attribute not in qt_context_attributes and attribute != target_attribute
                                      and attribute != 'entityId']
                for attribute in removal_attributes:
                    reduced_entity.pop(attribute)
                filtered_entities.append(reduced_entity)

            verified_evidences = []
            evidence_id = 0
            for entity in merged_entities:
                raw_evidence = entity_evidences[entity['entityId']]
                original_entity_id = raw_evidence['id']
                # First evidence --> Selected table, which will be removed from Table Corpus later on
                evidence = AugmentationEvidence(evidence_id, rqt_id, raw_evidence['entityId'],
                                    entity[target_attribute], raw_evidence['table'], raw_evidence['row_id'],
                                    target_attribute, None)
                evidence.signal = True
                evidence.scale = 3
                verified_evidences.append(evidence)
                evidence_id += 1

                # Find evidences from cluster
                found_cluster = None
                for cluster in collected_clusters:
                    if original_entity_id in [record['es_id'] for record in cluster['records']]:
                        found_cluster = cluster
                        break
                if found_cluster is None:
                    logging.warning('No cluster found for entity {}!'.format(original_entity_id))

                else:
                    hits = determine_cluster_hits(found_cluster['records'], schema_org_class)
                    for hit in hits:
                        if hit['id'] != original_entity_id and target_attribute in hit:
                            evidence = AugmentationEvidence(evidence_id, aqt_id, raw_evidence['entityId'],
                                                hit['table'], hit['row_id'], None,
                                                            hit[target_attribute], target_attribute)
                            evidence.signal = True
                            evidence.determine_scale(filtered_entities)
                            verified_evidences.append(evidence)
                            evidence_id += 1

            query_table = AugmentationQueryTable(aqt_id, 'augmentation', assembling_strategy,
                                                 gt_table, schema_org_class, qt_context_attributes, filtered_entities,
                                                 verified_evidences, target_attribute, use_case)
            query_table.save(with_evidence_context=False)
            aqt_id += 1


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    path_to_lspc_table_corpus_mappings = '{}/cluster/product/lspc2020_to_tablecorpus'.format(os.environ['DATA_DIR'])
    product_clusters = load_clusters('{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'filtered_product_clusters.json.gz'), None)

    # tables = set()
    # for cluster in product_clusters:
    #     for record in cluster['records']:
    #         tables.add(record['table_id'])
    #
    # print('{} tables in index'.format(len(tables)))

    target_attributes = ['brand', 'gtin12', 'sku']
    assemble_query_tables('product_iherb.com_september2020.json.gz', target_attributes, 'product', 2100, product_clusters)
    target_attributes = ['brand', 'sku']
    # assemble_query_tables('product_tommy.com_september2020.json.gz', target_attributes, 'product', 2200, product_clusters)
    # target_attributes = ['brand', 'sku']
    assemble_query_tables('product_rs-online.com_september2020.json.gz', target_attributes, 'product', 2300,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_zavvi.com_september2020.json.gz', target_attributes, 'product', 2400,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_communitymarkets.com_september2020.json.gz', target_attributes, 'product', 2500,
                          product_clusters)
    target_attributes = ['brand', 'sku', 'color', 'manufacturer']
    assemble_query_tables('product_zalando.de_september2020.json.gz', target_attributes, 'product', 2600,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_mackenthuns.com_september2020.json.gz', target_attributes, 'product', 2700,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_millsrecordcompany.com_september2020.json.gz', target_attributes, 'product', 2800,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_posterlounge.com_september2020.json.gz', target_attributes, 'product', 2900,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_dustinhome.se_september2020.json.gz', target_attributes, 'product', 3000,
                          product_clusters)
    target_attributes = ['brand', 'sku']
    assemble_query_tables('product_nzgameshop.com_september2020.json.gz', target_attributes, 'product', 3100,
                          product_clusters)
    target_attributes = ['brand', 'gtin13', 'manufacturer']
    assemble_query_tables('product_connox.com_september2020.json.gz', target_attributes, 'product', 3200,
                          product_clusters)
