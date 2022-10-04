import os

import logging
from random import randint

import pandas as pd
from tqdm import tqdm


def validate_training_data():
    path_to_fine_tuning_ds = '{}/fine-tuning/movie_fine-tuning_complete.csv'.format(os.environ['DATA_DIR'])
    df_finetuning_data = pd.read_csv(path_to_fine_tuning_ds, encoding='utf-8', sep=';')

    # Validate clusters
    validate_clusters(df_finetuning_data)

    # randomly validate 200 pairs
    false_annotated = 0
    no_cluster = df_finetuning_data['cluster'].max()
    for i in tqdm(range(0,200)):
        selected_cluster = randint(0, no_cluster)
        df_subset = df_finetuning_data[df_finetuning_data['cluster'] == selected_cluster]
        random_row_1 = df_subset.sample()

        random_row_2 = df_subset.sample()
        while random_row_1.index == random_row_2.index:
            random_row_2 = df_subset.sample()

        not_correct_input = True
        while not_correct_input:
            print('------------------------')
            print_row(random_row_1)
            print_row(random_row_2)
            answer_sim = input("Do the above entities describe the same entity? ")
            not_correct_input = not(answer_sim == 'y' or answer_sim == 'x')

        if answer_sim == 'y' and random_row_1['match_index'].values[0] != random_row_2['match_index'].values[0]:
            false_annotated += 1
            print('UPDATED false annotated')

        if answer_sim == 'x' and random_row_1['match_index'].values[0] == random_row_2['match_index'].values[0]:
            false_annotated += 1
            print('UPDATED false annotated')


    print('Found {} incorrect annotations'.format(false_annotated))


def print_row(row):
    for column in row.columns:
        print('{}\t{}'.format(column, row[column].values[0]))
    print('')


def validate_clusters(df_finetuning_data):
    """Check if all clusters have a size of 8"""
    no_clusters = df_finetuning_data['cluster'].max()
    for i in range(0, int(no_clusters)):
        df_sub_training = df_finetuning_data[df_finetuning_data['cluster'] == i]
        if len(df_sub_training) != 8:
            print('Cluster {} does not contain 8 rows.'.format(str(i)))

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    validate_training_data()