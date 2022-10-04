import itertools
import logging
import os

import pandas as pd
import textdistance


def merge_fine_tuning_results():

    path_to_fine_tuning_results = '{}/finetuning/open_book/magellan_results/'.format(os.environ['DATA_DIR'])
    df_merged_fine_tuning_results = None
    entities = ['entity1', 'entity2']
    attributes = ['name', 'addresslocality', 'addressregion', 'addresscountry', 'postalcode',
                  'streetaddress']

    for file_name in os.listdir(path_to_fine_tuning_results):
        if file_name == 'combined_results.csv':
            continue

        # Iterate through files in directory
        file_path = path_to_fine_tuning_results + file_name
        if df_merged_fine_tuning_results is None:
            df_merged_fine_tuning_results = pd.read_csv(file_path, sep=';', encoding='utf-8', index_col=0)

            # Rename prediction column to track prediction results of experiments
            pred_experiment_identifier = 'pred_{}'.format(extract_experiment_identifier(file_name))
            df_merged_fine_tuning_results.rename(columns={'pred': pred_experiment_identifier}, inplace=True)
        else:
            df_fine_tuning_results = pd.read_csv(file_path, sep=';', encoding='utf-8', index_col=0)

            # Check if all attributes are contained in merged_finetuning data frame
            for entity, attribute in itertools.product(entities, attributes):
                column_name = '{}_{}'.format(entity, attribute)
                if column_name not in df_merged_fine_tuning_results.columns \
                        and column_name in df_fine_tuning_results.columns:
                    df_merged_fine_tuning_results[column_name] = df_fine_tuning_results[column_name]

            # Experiment identifier
            pred_experiment_identifier = 'pred_{}'.format(extract_experiment_identifier(file_name))
            df_merged_fine_tuning_results[pred_experiment_identifier] = df_fine_tuning_results['pred']

    # Convert all columns to string
    df_merged_fine_tuning_results.fillna('', inplace=True)
    df_merged_fine_tuning_results = df_merged_fine_tuning_results.astype(str)

    # Merge attribute combinations
    for entity in entities:

        # Merge all attributes
        entity_attributes_all = ['{}_{}'.format(entity, attribute) for attribute in attributes]
        df_merged_fine_tuning_results['{}_all'.format(entity)] = df_merged_fine_tuning_results[entity_attributes_all].agg(' '.join, axis=1)

        # Merge name addresslocality
        entity_attributes_name_locality = ['{}_{}'.format(entity, attribute) for attribute in ['name', 'addresslocality']]
        df_merged_fine_tuning_results['{}_name_addresslocality'.format(entity)] = df_merged_fine_tuning_results[entity_attributes_name_locality].agg(
            ' '.join, axis=1)

    df_merged_fine_tuning_results.fillna('', inplace=True)

    # Calculate jaccard similarity of attribute combinations
    df_merged_fine_tuning_results['jaccard_all'] = \
        df_merged_fine_tuning_results.apply(lambda x: textdistance.jaccard.normalized_similarity(x['entity1_all'], x['entity2_all']), axis=1)
    df_merged_fine_tuning_results['jaccard_name_addresslocality'] = \
        df_merged_fine_tuning_results.apply(lambda x: textdistance.jaccard.normalized_similarity(x['entity1_name_addresslocality'], x['entity2_name_addresslocality']), axis=1)

    # Calculate levenshtein similarity of attribute combinations
    df_merged_fine_tuning_results['levenshtein_all'] = \
        df_merged_fine_tuning_results.apply(lambda x: textdistance.levenshtein.normalized_similarity(x['entity1_all'], x['entity2_all']), axis=1)
    df_merged_fine_tuning_results['levenshtein_name_addresslocality'] = \
        df_merged_fine_tuning_results.apply(lambda x: textdistance.levenshtein.normalized_similarity(x['entity1_name_addresslocality'], x['entity2_name_addresslocality']), axis=1)

    output_path = '{}/finetuning/open_book/magellan_results/combined_results.csv'.format(os.environ['DATA_DIR'])
    df_merged_fine_tuning_results.to_csv(output_path, sep=';', encoding='utf-8')

    print(len(df_merged_fine_tuning_results[df_merged_fine_tuning_results['entity1_all'] == df_merged_fine_tuning_results['entity2_all']]))


def extract_experiment_identifier(file_name):
    return file_name.replace('localbusiness_fine-tuning_', '').replace('.csv', '')

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    merge_fine_tuning_results()