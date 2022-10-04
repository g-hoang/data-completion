import itertools
import os

import logging
import random
from collections import Counter
from multiprocessing import Pool

import click
import pandas as pd
from tqdm import tqdm

from src.finetuning.open_book.serialize_entities import serialize_entities
from src.strategy.open_book.entity_serialization import EntitySerializer


@click.command()
@click.option('--schema_org_class')
@click.option('--worker', type=int, default=0)
def split_finetuning_data(schema_org_class, worker):
    """Split data set and prepare for fine-tuning with sbert/ supcon/ dpr"""
    logger = logging.getLogger()
    random.seed(42)
    logger.info('Split finetuning data for schema org class {}'.format(schema_org_class))

    # TO-DO: Move naming of DS to parameter!
    path_to_fine_tuning_ds = '{}/finetuning/open_book/{}_fine-tuning_complete_extended_subset_pairs.csv'.format(os.environ['DATA_DIR'], schema_org_class)
    dtypes = {'addresslocality': str, 'name': str}
    df_training_data = pd.read_csv(path_to_fine_tuning_ds, encoding='utf-8', sep=';', dtype=dtypes)
    logger.info('DS loaded with {} entities'.format(len(df_training_data)))

    max_cluster_id = df_training_data['cluster'].max()
    no_unique_cluster = len(df_training_data['cluster'].unique())
    logger.info('Found {} clusters in training data'.format(no_unique_cluster))
    #no_clusters = 5
    # Select split
    options = ['train', 'dev']
    weights = [0.8, 0.2]
    cluster_to_split = random.choices(options, weights, k=max_cluster_id + 1)
    print(Counter(cluster_to_split))

    serialization_methods = ['dpr', 'standard']
    if schema_org_class == 'localbusiness':
        context_attributes = ['name', 'addresslocality']
    elif schema_org_class == 'product':
        context_attributes = ['name']
    else:
        raise ValueError('Schema Org class {} is unknown!'.format(schema_org_class))
    df_dpr = pd.DataFrame()
    df_finetuning = pd.DataFrame()

    if worker > 0:
        results = []
        pool = Pool(worker)

    for current_cluster_id in tqdm(df_training_data['cluster'].unique()):

        df_sub_training = df_training_data.loc[df_training_data['cluster'] == current_cluster_id]
        if len(df_sub_training) == 0:
            continue

        split = cluster_to_split[current_cluster_id]

        if worker == 0:

            sub_training_data = \
                prepare_fine_tuning_data_for_all_methods(schema_org_class, context_attributes, df_sub_training,
                                                         current_cluster_id,
                                                         split, serialization_methods)

            for method in serialization_methods:
                if method == 'dpr':
                    # Prepare dpr fine-tuning data
                    df_dpr = pd.concat([df_dpr, sub_training_data[method]], ignore_index=True)
                elif method == 'standard':
                    df_finetuning = pd.concat([df_finetuning, sub_training_data[method]], ignore_index=True)
        elif worker > 0:
            results.append(
                pool.apply_async(prepare_fine_tuning_data_for_all_methods, (schema_org_class, context_attributes,
                                                                            df_sub_training,
                                                                            current_cluster_id, split,
                                                                            serialization_methods,)))


    if worker > 0:
        process_bar = tqdm(total=len(results))
        while True:
            if len(results) == 0:
                break

            collected_results = []
            dpr_dfs = [df_dpr]
            finetuning_dfs = [df_finetuning]
            for result in results:
                if result.ready():
                    sub_training_data = result.get()

                    for method in serialization_methods:
                        if method == 'dpr':
                            dpr_dfs.append(sub_training_data[method])
                        elif method == 'standard':
                            finetuning_dfs.append(sub_training_data[method])
                        else:
                            logger.warning('Split Method {} is not defined!'.format(method))

                    collected_results.append(result)
                    process_bar.update(1)

            df_dpr = pd.concat(dpr_dfs, ignore_index=True)

            df_finetuning = pd.concat(finetuning_dfs, ignore_index=True)

            results = [result for result in results if result not in collected_results]
        process_bar.close()

    # Split SBERT and Cross Encoder data set
    options = ['train', 'dev']
    weights = [0.8, 0.2]
    df_finetuning['split'] = random.choices(options, weights, k=len(df_finetuning))

    # Save Data Sets
    path_to_fine_tuning_split = '{}/finetuning/open_book/{}_fine-tuning_dpr_extended_subset_pairs.csv'.format(os.environ['DATA_DIR'],
                                                                                        schema_org_class)
    df_dpr.to_csv(path_to_fine_tuning_split, sep=';', encoding='utf-8', index=False)
    logger.info('DPR finetuning data saved')

    # Exclude non matching pairs for supcon loss
    df_supcon = df_dpr[df_dpr['match_index'] == 1]
    df_supcon = df_supcon.drop(columns=['match_index', 'split'])

    path_to_fine_tuning_split = '{}/finetuning/open_book/{}_fine-tuning_supcon_extended_subset_pairs.pkl.gz'.format(os.environ['DATA_DIR'],
                                                                                        schema_org_class)

    df_supcon.to_pickle(path_to_fine_tuning_split, compression='gzip')
    path_to_fine_tuning_split = '{}/finetuning/open_book/{}_fine-tuning_supcon_extended_subset_pairs.csv'.format(
        os.environ['DATA_DIR'],
        schema_org_class)
    df_supcon.to_csv(path_to_fine_tuning_split, sep=';', encoding='utf-8')
    logger.info('Supcon finetuning data saved')


    path_to_fine_tuning_split = '{}/finetuning/open_book/{}_fine-tuning_extended_subset_pairs.csv'.format(os.environ['DATA_DIR'],
                                                                                      schema_org_class)
    df_finetuning.to_csv(path_to_fine_tuning_split, sep=';', encoding='utf-8', index=False, float_format='%.0f')
    logger.info('General finetuning data saved')

    # Serialize & Save records - sbert
    serialize_entities(schema_org_class, df_finetuning, 'sbert')
    # Serialize & Save records - cross encoder
    serialize_entities(schema_org_class, df_finetuning, 'cross')



def prepare_fine_tuning_data_for_all_methods(schema_org_class, context_attributes, df_sub_training, current_cluster_id, split,
                                             serialization_methods):
    """Prepare Finetuning data for all defined methods"""
    sub_training_data = {}
    for method in serialization_methods:
        sub_training_data[method] = prepare_fine_tuning_data(schema_org_class, context_attributes, df_sub_training, current_cluster_id,
                                                             split, method)

    return sub_training_data


def prepare_fine_tuning_data(schema_org_class, context_attributes, df_sub_training, current_cluster_id, split, method):

    if method == 'dpr':
        # Prepare dpr fine-tuning data
        df_sub = prepare_dpr_fine_tuning_data(schema_org_class, context_attributes, df_sub_training, split)
    elif method == 'standard':
        # Prepare fine-tuning data
        df_sub = prepare_standard_fine_tuning_data(schema_org_class, df_sub_training, current_cluster_id)

    else:
        raise ValueError('Method {} is not defined'.format(method))

    return df_sub


def prepare_dpr_fine_tuning_data(schema_org_class, context_attributes, df_sub_training, split):

    entity_serializer = EntitySerializer(schema_org_class, context_attributes)
    df_dpr = pd.DataFrame()

    # Prepare dpr fine-tuning data
    row_id = 1
    for index, row in df_sub_training.iterrows():
        if row['match_index'] == 1:
            record = {'id': row_id, 'cluster_id': row['cluster'], 'split': split, 'match_index': row['match_index']}

            record['features'] = entity_serializer.convert_to_str_representation(row)
            df_dpr = pd.concat([df_dpr, pd.DataFrame(record, index=[row_id])], ignore_index=True)
            row_id += 1

    return df_dpr


def prepare_standard_fine_tuning_data(schema_org_class, df_sub_training, current_cluster_id):
    entity_serializer = EntitySerializer(schema_org_class)
    df_fine_tuning = pd.DataFrame()

    count = 0

    used_match_indices = []
    for id1, id2 in itertools.combinations(df_sub_training.index, 2):

        if len(df_fine_tuning) >= 20:
            # Add only 20 matches per cluster
            break

        row_1 = df_sub_training.loc[id1]
        row_2 = df_sub_training.loc[id2]

        record = {'cluster': current_cluster_id}

        # For localbusiness match_index = 1 is always an entity from a cluster.
        # All pairs should be related to the clusters
        if row_1['match_index'] != 1 and row_1['match_index'] != 1:
            continue

        if row_1['match_index'] != 1 and row_1['match_index'] != 1:
            continue
        # Use non cluster entities only once
        if row_1['match_index'] != 1 and row_1['match_index'] in used_match_indices:
            continue
        else:
            used_match_indices.append(row_1['match_index'])
        # Use non cluster entities only once
        if row_2['match_index'] != 1 and row_2['match_index'] in used_match_indices:
            continue
        else:
            used_match_indices.append(row_2['match_index'])

        # Determine score
        record['score'] = 1 if row_1['match_index'] == row_2['match_index'] else 0

        # Convert entities
        encoded_entity_1 = entity_serializer.convert_to_str_representation(row_1)
        encoded_entity_2 = entity_serializer.convert_to_str_representation(row_2)

        # Double check that representations do not exactly match for none matches
        if record['score'] == 0 and encoded_entity_1 == encoded_entity_2:
            continue

        projected_entity_1 = entity_serializer.project_entity(row_1)
        for attribute in projected_entity_1:
            record['entity1_{}'.format(attribute)] = projected_entity_1[attribute]

        projected_entity_2 = entity_serializer.project_entity(row_2)
        for attribute in projected_entity_2:
            record['entity2_{}'.format(attribute)] = projected_entity_2[attribute]

        df_fine_tuning = pd.concat([df_fine_tuning, pd.DataFrame(record, index=[count])], ignore_index=True)
        df_fine_tuning = df_fine_tuning.drop_duplicates()
        count += 1

    return df_fine_tuning


if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    split_finetuning_data()
