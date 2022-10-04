import json
import logging
import os

import click
import pandas as pd
from elasticsearch import Elasticsearch
from pandas import Series
from tqdm import tqdm
from random import randint

from src.similarity.string_comparator import string_similarity
from src.strategy.open_book.retrieval.retrieval_strategy import RetrievalStrategy


@click.command()
@click.option('--schema_org_class')
def generate_training_data(schema_org_class):
    """Generate Training data"""

    #seed(42)
    # Connect to Elasticsearch
    _es = Elasticsearch([{'host': os.environ['ES_INSTANCE'], 'port': 9200}])
    _es.ping()

    # Load pre-training data pools
    path_to_train_data_pool = '{}/fine-tuning/Movie_imdb.com_September2020.json'.format(os.environ['DATA_DIR'])

    loaded_data = []
    with open(path_to_train_data_pool, 'r', encoding="utf-8") as file:
        for line in tqdm(file.readlines()):
            line = json.loads(line)
            if len(line.keys()) > 1:
                if line['director'] is not None and 'name' in line['director']:
                    line['director'] = line['director']['name']
                for key in line.keys():
                    if key in ['name', 'director', 'datepublished', 'duration'] and type(line[key]) is list:
                        line[key] = ', '.join([value['name'] if type(value) is dict else value for value in line[key]])
                loaded_data.append(line)

    df_loaded = pd.DataFrame(loaded_data, columns=['name', 'director', 'datepublished', 'duration'])
    df_loaded = df_loaded.drop_duplicates(subset=['name', 'datepublished'])

    path_to_fine_tuning_ds = '{}/fine-tuning/movie_fine-tuning_complete.csv'.format(os.environ['DATA_DIR'])
    pd_training_data = pd.read_csv(path_to_fine_tuning_ds, encoding='utf-8', sep=';')
    pd_training_data['match_index'] = pd_training_data['match_index'].astype('float32')
    pd_training_data = pd_training_data[pd_training_data['match_index'] < 4]
    cluster_offset = pd_training_data['cluster'].max()

    for i in range(0, 1):
        # Initialize Data Frame
        pd_new_cluster = pd.DataFrame()
        match_index = 0 # Count up to 8

        anchor_row, sim_corner_case, dis_corner_case = search_anchor_and_corner_cases(df_loaded, schema_org_class)
        anchor_row['match_index'] = match_index
        pd_new_cluster = pd_new_cluster.append(anchor_row)
        dissimilar_rows = [anchor_row]

        not_correct_input = True
        while not_correct_input:
            print('------------------------')
            print(anchor_row)
            print(sim_corner_case)
            answer_sim = input("Do the above entities describe the same entity? ")
            not_correct_input = not(answer_sim == 'y' or answer_sim == 'x')

        not_correct_input = True
        while not_correct_input:
            print('------------------------')
            print(anchor_row)
            print(dis_corner_case)
            answer_dsim = input("Do the above entities describe the same entity? ")
            not_correct_input = not (answer_dsim == 'y' or answer_dsim == 'x')

        if answer_dsim == 'y' and answer_sim == 'y':
            # Corner case --> dissimilar entity is actually similar
            dis_corner_case['match_index'] = match_index
            pd_new_cluster = pd_new_cluster.append(dis_corner_case, ignore_index=True)
        elif answer_dsim == 'y' and answer_sim == 'n':
            # Corner case --> similar entity is not similar
            dis_corner_case['match_index'] = match_index
            pd_new_cluster = pd_new_cluster.append(dis_corner_case, ignore_index=True)

            match_index += 1
            sim_corner_case_2 = manipulate_row(sim_corner_case)
            sim_corner_case['match_index'] = match_index
            sim_corner_case_2['match_index'] = match_index
            dissimilar_rows.append(sim_corner_case)
            pd_new_cluster = pd_new_cluster.append(sim_corner_case, ignore_index=True)
            pd_new_cluster = pd_new_cluster.append(sim_corner_case_2, ignore_index=True)

        elif answer_sim == 'y' and answer_dsim == 'x':
            # Corner case --> dissimilar entity is not similar
            sim_corner_case['match_index'] = match_index
            pd_new_cluster = pd_new_cluster.append(sim_corner_case, ignore_index=True)

            match_index += 1
            dis_corner_case_2 = manipulate_row(dis_corner_case)
            dis_corner_case['match_index'] = match_index
            dis_corner_case_2['match_index'] = match_index
            dissimilar_rows.append(dis_corner_case)
            pd_new_cluster = pd_new_cluster.append(dis_corner_case, ignore_index=True)
            pd_new_cluster = pd_new_cluster.append(dis_corner_case_2, ignore_index=True)

        elif answer_sim == 'x' and answer_dsim == 'x':
            # Both are not similar
            anchor_row_2 = manipulate_row(anchor_row)
            anchor_row_2['match_index'] = match_index
            pd_new_cluster = pd_new_cluster.append(anchor_row_2)

            match_index += 1
            sim_corner_case_2 = manipulate_row(sim_corner_case)
            sim_corner_case['match_index'] = match_index
            sim_corner_case_2['match_index'] = match_index
            dissimilar_rows.append(sim_corner_case)
            pd_new_cluster = pd_new_cluster.append(sim_corner_case, ignore_index=True)
            pd_new_cluster = pd_new_cluster.append(sim_corner_case_2, ignore_index=True)

        while match_index < 3:
            match_index += 1
            # Select random row and manipulate it
            random_row_1 = select_random_none_matching_row(df_loaded, dissimilar_rows)
            random_row_2 = manipulate_row(random_row_1)

            dissimilar_rows.append(random_row_1)

            random_row_1['match_index'] = match_index
            random_row_2['match_index'] = match_index

            pd_new_cluster = pd_new_cluster.append(random_row_1)
            pd_new_cluster = pd_new_cluster.append(random_row_2)


        # Assign cluster
        pd_new_cluster['cluster'] = i + 1 + cluster_offset
        pd_training_data = pd_training_data.append(pd_new_cluster)
        pd_training_data.to_csv(path_to_fine_tuning_ds, sep=';', encoding='utf-8', index=False)


def search_anchor_and_corner_cases(df_loaded, schema_org_class):
    """Search for an anchor row as well as suitable similar/ dissimilar corner cases"""
    strategy = RetrievalStrategy(schema_org_class)
    sim_corner_case = None
    dsim_corner_case = None
    anchor_row = None

    while sim_corner_case is None or dsim_corner_case is None:
        sim_corner_case = None
        dsim_corner_case = None

        # Select anchor row
        anchor_row = df_loaded.iloc[randint(0, len(df_loaded))]

        # 1 --> Fetch matching entity from elastic search index
        entity_result = strategy.query_tables_index(anchor_row, None, 50,
                                                    'normalized_entity_index_movie_bert-base-uncased')

        for hit in entity_result['hits']['hits']:
            if 'director' in hit['_source'] and 'director' in anchor_row:
                sim = 0.7 * string_similarity(hit['_source']['name'], anchor_row['name']) \
                      + 0.3 * string_similarity(hit['_source']['director'], anchor_row['director'])
            elif 'datepublished' in hit['_source'] and 'datepublished' in anchor_row:
                sim = 0.7 * string_similarity(hit['_source']['name'], anchor_row['name']) \
                      + 0.3 * string_similarity(hit['_source']['datepublished'], anchor_row['datepublished'])
            elif 'duration' in hit['_source'] and 'duration' in anchor_row:
                sim = 0.7 * string_similarity(hit['_source']['name'], anchor_row['name']) \
                      + 0.3 * string_similarity(hit['_source']['duration'], anchor_row['duration'])
            else:
                sim = string_similarity(hit['_source']['name'], anchor_row['name'])
            if sim < 0.95 and sim > 0.8:
                if sim_corner_case is None:
                    sim_corner_case = hit['_source']
            elif sim < 0.7 and sim > 0.5:
                if dsim_corner_case is None:
                    dsim_corner_case = hit['_source']
            elif sim < 0.5:
                break

            if sim_corner_case is not None and dsim_corner_case is not None:
                break

    # Postprocessing - keep only relevant attributes
    kept_attributes = ['director', 'datepublished', 'duration']
    norm_sim_corner_case = Series({'name': sim_corner_case['name']})
    for attr in kept_attributes:
        if attr in sim_corner_case:
            norm_sim_corner_case[attr] = sim_corner_case[attr]

    norm_dsim_corner_case = Series({'name': dsim_corner_case['name']})
    for attr in kept_attributes:
        if attr in dsim_corner_case:
            norm_dsim_corner_case[attr] = dsim_corner_case[attr]

    return anchor_row, norm_sim_corner_case, norm_dsim_corner_case


def select_random_none_matching_row(df_loaded, rows):
    """Select a random row from the corpus that does not match the anchor row"""
    matching = True
    random_row = None
    while matching:
        random_row = df_loaded.iloc[randint(0, len(df_loaded))]
        for row in rows:
            if not matching:
                break
            matching = random_row['name'] == row['name'] and random_row['datepublished'] == row['datepublished']

    return random_row


def manipulate_row(row):
    """Manipulate given row"""
    manipulated_row = row.copy()
    columns = row.keys()

    # Delete random dict value
    selected_col = columns[randint(0, len(columns) - 1)]
    if selected_col != 'name':
        del manipulated_row[selected_col]

    # Delete second random row ~33%
    columns = manipulated_row.keys()
    if len(columns) > 2 and randint(0, 2) == 1:
        selected_col = columns[randint(0, len(columns) - 1)]
        if selected_col != 'name':
            del manipulated_row[selected_col]

    return manipulated_row


if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    generate_training_data()
