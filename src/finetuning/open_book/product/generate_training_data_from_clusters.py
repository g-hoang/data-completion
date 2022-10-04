import logging
import os
import random
from multiprocessing import Pool

import click
import pandas as pd
from tqdm import tqdm

from src.data.product.load_clusters import load_clusters
from src.model.querytable_new import load_query_tables_by_class
from src.similarity.coordinate import haversine
from src.similarity.string_comparator import string_similarity, string_containment
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.query_by_entity import QueryByEntity


@click.command()
@click.option('--schema_org_class', default='product')
@click.option('--worker', default=0, type=int, help='Run parallel processing if worker > 0')
def generate_training_data_from_clusters(schema_org_class, worker):
    random.seed(42)

    # Only context attributes
    attributes = ['name']

    # Prepare output file
    path_to_finetuning_collection = '{}/finetuning/open_book/{}_fine-tuning_complete_extended_subset_pairs.csv'.format(os.environ['DATA_DIR'],
                                                                                                     schema_org_class)

    # Load Clusters
    path_to_lspc_table_corpus_mappings = '{}/cluster/product/lspc2020_to_tablecorpus'.format(os.environ['DATA_DIR'])
    product_clusters = load_clusters('{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'filtered_product_clusters.json.gz'), None)

    # Filter cluster by no_tables > 1 and cluster size <= 20
    product_clusters = [cluster for cluster in product_clusters if len(cluster['tables']) > 1 and len(cluster['records']) <= 20]

    # Load query tables
    querytables = load_query_tables_by_class('retrieval', schema_org_class)

    if worker > 0:
        # Prepare parallel processing
        results = []
        pool = Pool(worker)

    # Create one chunk of clusters for each worker
    def chunks(lst, worker):
        """Yield successive sized chunks from lst."""
        chunk_size = int(len(lst)/ worker) + 1
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    # Select subset of clusters that is contained in the query tables
    #clusters_in_query_tables =

    df_fine_tuning = pd.DataFrame()

    no_clusters = {'yes_head': 50, 'yes_tail': 150, 'no_head': 2000, 'no_tail': 6000} # 600 known & unknown clusters
    known_rows = []
    for querytable in querytables:
        new_known_rows = [(verified_evidence.table, verified_evidence.row_id)
                     for verified_evidence in querytable.verified_evidences]
        known_rows.extend(new_known_rows)

    if worker == 0:
        df_cluster_fine_tuning = generate_training_data_by_cluster(product_clusters, schema_org_class, attributes, known_rows, no_clusters)
        df_fine_tuning = pd.concat([df_fine_tuning, df_cluster_fine_tuning])
    elif worker > 0:
        random.shuffle(product_clusters)
        results = []
        for chunk_cluster in chunks(product_clusters, worker):
            results.append(
                pool.apply_async(
                    generate_training_data_by_cluster, (chunk_cluster, schema_org_class, attributes, known_rows, no_clusters,)))

    if worker > 0:
        #process_bar = tqdm(total=len(results))
        while True:
            if len(results) == 0:
                break

            collected_results = []
            fine_tuning_dfs = [df_fine_tuning]
            for result in results:
                if result.ready():
                    new_fine_tuning_dfs = result.get()
                    fine_tuning_dfs.extend(new_fine_tuning_dfs)
                    collected_results.append(result)
                    #process_bar.update(1)

            df_fine_tuning = pd.concat(fine_tuning_dfs)
            results = [result for result in results if result not in collected_results]
        #process_bar.close()

    df_fine_tuning.drop_duplicates()
    df_fine_tuning.to_csv(path_to_finetuning_collection, encoding='utf-8', sep=';')

    logging.info('Generated training data with {} entities'.format(len(df_fine_tuning)))


def generate_training_data_by_cluster(clusters, schema_org_class, attributes, known_rows, no_clusters):
    """Generate training data based on provided cluster"""
    random.seed(42)
    query_strategy = QueryByEntity(schema_org_class)
    index_name = determine_es_index_name(schema_org_class, clusters=True)
    no_entities = query_strategy.get_no_index_entities(index_name)
    fine_tuning_dfs = []
    random.shuffle(clusters)

    # Make sure to produce  50% pairs, which are known &
    #                       50% pairs, which are not known
    cluster_characteristics = {'yes_head': 0, 'yes_tail': 0, 'no_head': 0, 'no_tail': 0}

    for cluster in tqdm(clusters):

        # check if enough training data was found
        if sum([value for value in cluster_characteristics.values()]) == sum([value for value in no_clusters.values()]):
            break

        # constraint to max 30 records per cluster
        sub_set_cluster_records = cluster['records']
        if len(cluster['records']) > 30:
            sub_set_cluster_records = random.sample(cluster['records'], 30)

        # Check if any of the hits is known
        known = False
        for record in sub_set_cluster_records:
            record_row = (record['table_id'], record['row_id'])
            if record_row in known_rows:
                known = True
                break

        head_entity = len(sub_set_cluster_records) >= 5

        if (known and head_entity and cluster_characteristics['yes_head'] < no_clusters['yes_head']) \
            or (known and not head_entity and cluster_characteristics['yes_tail'] < no_clusters['yes_tail']) \
                or (not known and head_entity and cluster_characteristics['no_head'] < no_clusters['no_head']) \
                    or (not known and not head_entity and cluster_characteristics['no_tail'] < no_clusters['no_tail']):

            if known and head_entity:
                cluster_characteristics['yes_head'] += 1
            elif known and not head_entity:
                cluster_characteristics['yes_tail'] += 1
            elif not known and head_entity:
                cluster_characteristics['no_head'] += 1
            elif not known and not head_entity:
                cluster_characteristics['no_tail'] += 1

            # Write results to file
            record_dfs = []
            match_index_non_matches = 2
            cluster_ids = ['{}-{}'.format(record['table_id'], record['row_id']) for record in sub_set_cluster_records]

            for record in sub_set_cluster_records:
                hit = query_strategy.query_tables_index_by_table_row_id(record['table_id'], record['row_id'], index_name)

                if hit is not None:
                    record = {'cluster': cluster['cluster_id'], 'match_index': 1,
                              'table': hit['table'],
                              'row_id': hit['row_id']}
                    for attribute in attributes:
                        if attribute in hit:
                            record[attribute] = hit[attribute]

                    record_dfs.append(pd.DataFrame(record, index=[hit['_id']]))
                    # Add random example in 50% of the cases
                    if random.randint(0, 1) == 1:
                        # not_found_id = True
                        # random_id = -1
                        # while not_found_id and random_id < 0:
                        #     random_id =
                        #     # Check if record was not already already used for this cluster
                        #     not_found_id = random_id not in used_non_matching_records
                        random_id = random.randint(0, no_entities)
                        random_hits = query_strategy.query_tables_index_by_id([random_id], index_name)
                        for random_hit in random_hits['hits']['hits']:
                            # Compare strings - Only name & addressLocality
                            hit_identifier = '{}-{}'.format(random_hit['_source']['table'], random_hit['_source']['row_id'])
                            if hit_identifier not in cluster_ids:
                                # hit_string = create_string_from_hit(hit)
                                # random_hit_string = create_string_from_hit(random_hit['_source'])
                                #
                                # if string_similarity(hit_string, random_hit_string) < 0.7 \
                                #         and not string_containment(hit_string, random_hit_string):
                                record = {'cluster': cluster['cluster_id'], 'match_index': match_index_non_matches,
                                          'table': random_hit['_source']['table'],
                                          'row_id': random_hit['_source']['row_id']}
                                for attribute in attributes:
                                    if attribute in random_hit['_source']:
                                        record[attribute] = random_hit['_source'][attribute]
                                record_dfs.append(
                                    pd.DataFrame(record, index=[
                                        '{}-{}'.format(random_hit['_id'], match_index_non_matches)]))

                                cluster_ids.append(hit_identifier)

                                match_index_non_matches += 1
                                break

                    # Add similar example in 50% of the cases - use telephone number and geo coordinates for disambiguation
                    else:
                        similar_hits = query_strategy.query_tables_index(hit, 'name', 30, index_name)
                        for similar_hit in similar_hits['hits']['hits']:
                            hit_identifier = '{}-{}'.format(similar_hit['_source']['table'],
                                                            similar_hit['_source']['row_id'])
                            if hit_identifier not in cluster_ids:
                            # try:
                            #     if similar_hit['_id'] not in cluster['records'] \
                            #             and similar_hit['_id'] not in used_non_matching_records \
                            #             and 'longitude' in similar_hit['_source'] and 'latitude' in similar_hit[
                            #         '_source'] \
                            #             and 'longitude' in hit and 'latitude' in hit \
                            #             and hit['longitude'] != similar_hit['_source']['longitude'] \
                            #             and hit['latitude'] != similar_hit['_source']['latitude'] \
                            #             and haversine(float(hit['longitude']), float(hit['latitude']),
                            #                           float(similar_hit['_source']['longitude']),
                            #                           float(similar_hit['_source']['latitude'])) > 0.3:
                            #
                            #         # Compare strings - Only name & addressLocality
                            #     hit_string = create_string_from_hit(hit)
                            #     similar_hit_string = create_string_from_hit(similar_hit['_source'])
                            #
                            #     if string_similarity(hit_string, similar_hit_string) < 0.7 \
                            #             and not string_containment(hit_string, similar_hit_string):
                                    record = {'cluster': cluster['cluster_id'],
                                              'match_index': match_index_non_matches,
                                              'table': similar_hit['_source']['table'],
                                              'row_id': similar_hit['_source']['row_id']}
                                    for attribute in attributes:
                                        if attribute in similar_hit['_source']:
                                            record[attribute] = similar_hit['_source'][attribute]

                                    record_dfs.append(
                                        pd.DataFrame(record,
                                                     index=['{}-{}'.format(similar_hit['_id'],
                                                                           match_index_non_matches)]))
                                    cluster_ids.append(hit_identifier)

                                    match_index_non_matches += 1
                                    break
                            # except (TypeError, ValueError) as e:
                            #     logging.warning(e)

            df_cluster_new_fine_tuning = pd.concat(record_dfs)
            fine_tuning_dfs.append(df_cluster_new_fine_tuning)

    logging.info('Known head clusters {} - known tail clusters: {}'.format(cluster_characteristics['yes_head'], cluster_characteristics['yes_tail']))
    logging.info('Unknown head clusters {} - unknown tail clusters: {}'.format(cluster_characteristics['no_head'],
                                                                           cluster_characteristics['no_tail']))
    return pd.concat(fine_tuning_dfs)

def create_string_from_hit(hit):
    new_string = hit['name']

    return new_string


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    generate_training_data_from_clusters()
