import logging
import os
import time

from tqdm import tqdm

from src.model.querytable_new import get_gt_tables, get_query_table_paths, load_query_table_from_file
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity
from src.data.localbusiness.load_clusters import load_clusters as load_localbusiness_clusters
from src.data.product.load_clusters import load_clusters as load_product_clusters


def check_existing_records():
    logger = logging.getLogger()

    # Load query tables
    reduced_rows = 0
    total_rows = 0

    schema_org_class = 'localbusiness'

    gt_tables = get_gt_tables('retrieval', schema_org_class)

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


    for gt_table in gt_tables:
        query_table_paths = get_query_table_paths('retrieval', schema_org_class, gt_table)
        query_tables = [load_query_table_from_file(path) for path in query_table_paths]


        for query_table in query_tables:
            double_checked_evidences = []
            for evidence in query_table.verified_evidences:
                table = evidence.table.lower().split('_')[1]
                if table[:3] in clusters and table in clusters[table[:3]] and evidence.row_id in clusters[table[:3]][table]:
                    double_checked_evidences.append(evidence)

            new_evidence_id = 0
            found_entity_ids = set()
            for evidence in double_checked_evidences:
                evidence.identifier = new_evidence_id
                found_entity_ids.add(evidence.entity_id)
                new_evidence_id += 1

            query_table.verified_evidences = double_checked_evidences

            #Remove not found records
            removable_rows = []
            for row in query_table.table:
                if row['entityId'] not in found_entity_ids:
                    removable_rows.append(row)

            query_table.table = [row for row in query_table.table if row not in removable_rows]
            query_table.save(with_evidence_context=True)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger()

    # Load environmental parameters
    path_to_data = os.environ['DATA_DIR']

    check_existing_records()

    logger.info('Checked records of query tables!')
