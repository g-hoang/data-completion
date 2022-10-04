import logging
import os

import click
import pandas as pd

from src.finetuning.open_book.localbusiness.derive_magellan_training_data import deserialize_entity
from src.strategy.open_book.ranking.similarity.similarity_re_ranking_factory import select_similarity_re_ranker


@click.command()
@click.option('--schema_org_class')
def weight_entities_for_sbert_using_magellan(schema_org_class):
    dataset_path = '{}/finetuning/open_book/{}_fine-tuning_sbert_new.csv'.format(os.environ['DATA_DIR'],
                                                                                          schema_org_class)

    df_sbert = pd.read_csv(dataset_path, sep=';', encoding='utf-8')
    df_sbert_matches = df_sbert.loc[df_sbert['score'] == 1].copy()


    # Create missing value matches
    df_sbert_matches_entity1 = deserialize_entity(df_sbert_matches['entity1'], 'entity1')
    df_sbert_matches = df_sbert_matches.join(df_sbert_matches_entity1)

    df_sbert_matches_entity2 = deserialize_entity(df_sbert_matches['entity2'], 'entity2')
    df_sbert_matches = df_sbert_matches.join(df_sbert_matches_entity2)

    #df_sbert_matches = df_sbert_matches.loc[~((df_sbert_matches['entity1_addresslocality'].isnull())
    #                                  | (df_sbert_matches['entity2_addresslocality'].isnull()))]

    # Create missing addresslocality examples
    #df_sbert_matches1 = df_sbert_matches[:int(len(df_sbert_matches) / 2)]
    #df_sbert_matches2 = df_sbert_matches[int(len(df_sbert_matches) / 2):]
    #df_sbert_matches1['entity1'] = df_sbert_matches1['entity1'].str.split(pat="\[COL\]addresslocality", expand=True)[0]
    #df_sbert_matches2['entity2'] = df_sbert_matches2['entity2'].str.split(pat="\[COL\]addresslocality", expand=True)[0]
    #df_sbert_matches1['score'] = 0.8
    #df_sbert_matches2['score'] = 0.8

    # Create exact matching examples
    df_sbert_exact_matches1 = df_sbert_matches.copy()
    df_sbert_exact_matches1['entity1'] = df_sbert_exact_matches1['entity2'].sample(frac=0.2)
    df_sbert_exact_matches2 = df_sbert_matches.copy()
    df_sbert_exact_matches2['entity2'] = df_sbert_exact_matches2['entity1'].sample(frac=0.2)

    #df_sbert_matches1 = df_sbert_matches1.drop(['entity1_name', 'entity1_addresslocality', 'entity2_name', 'entity2_addresslocality'], axis=1)
    #df_sbert_matches2 = df_sbert_matches2.drop(['entity1_name', 'entity1_addresslocality', 'entity2_name', 'entity2_addresslocality'], axis=1)
    df_sbert_exact_matches1 = df_sbert_exact_matches1.drop(['entity1_name', 'entity1_addresslocality', 'entity2_name', 'entity2_addresslocality'], axis=1)
    df_sbert_exact_matches2 = df_sbert_exact_matches2.drop(['entity1_name', 'entity1_addresslocality', 'entity2_name', 'entity2_addresslocality'], axis=1)

    #df_sbert = pd.concat([df_sbert, df_sbert_matches1, df_sbert_matches2, df_sbert_exact_matches1, df_sbert_exact_matches2], ignore_index=True)
    df_sbert = pd.concat([df_sbert, df_sbert_exact_matches1, df_sbert_exact_matches2], ignore_index=True)
    df_entity1 = deserialize_entity(df_sbert['entity1'], 'entity1')
    df_entity1.columns = [column.replace('entity1_', '') for column in df_entity1.columns]

    df_entity2 = deserialize_entity(df_sbert['entity2'], 'entity2')
    df_entity2.columns = [column.replace('entity2_', '') for column in df_entity2.columns]

    # Determine corner cases of evidences using RF from Magellan
    #   Load three Magellan re-ranker
    #re_ranking_strategy = {'name': 'magellan_re_ranker', 'model_name': 'RF'}
    # Use fixed context attribute combinations to be as consistent as possible across all query tables
    #context_attributes = ['name', 'addresslocality']

    # Initialize magellan re-ranker
    #mg_re_ranker = select_similarity_re_ranker(re_ranking_strategy, 'localbusiness', context_attributes)
    #df_sbert[['pred', 'proba']] = mg_re_ranker.predict_matches(df_entity1.to_dict('records'), df_entity2.to_dict('records'))

    #print(len(df_sbert[(df_sbert['score'] == 1) & (df_sbert['pred'] == 1) & (df_sbert['proba'] < 0.9)]))
    #print(len(df_sbert[(df_sbert['score'] == 0) & (df_sbert['pred'] == 1)]))
    #df_sbert.loc[(df_sbert['score'] == 1) & (df_sbert['pred'] == 1) & (df_sbert['proba'] < 0.9), 'score'] = 0.8
    #df_sbert.loc[(df_sbert['score'] == 0) & (df_sbert['pred'] == 1), 'score'] = 0.2

    # Check for Missing Addresslocality values
    #df_sbert = df_sbert.loc[~((df_entity1['addresslocality'].isnull()) & (df_entity2['addresslocality'].isnull()))]
    #df_sbert = df_sbert.drop_duplicates()

    df_sbert_drops = df_sbert.loc[df_sbert['score'] == 0].copy().sample(frac=0.5)
    df_sbert = df_sbert.drop(index=df_sbert_drops.index)
    #df_sbert.loc[(df_sbert['score'] > 0) &
    #             ((df_entity1['addresslocality'].isnull()) | (df_entity2['addresslocality'].isnull())), 'score'] -= 0.2

    #df_sbert.loc[(df_sbert['score'] > 0) & (df_sbert['pred'] == 1) & (df_sbert['proba'] < 0.9), 'score'] = 0.8

    #df_sbert.drop(['aggregated_predictions'], axis=1)
    output_dataset_path = '{}/finetuning/open_book/{}_fine-tuning_sbert_weight_corner_cases_missing_values_7.csv'.format(
        os.environ['DATA_DIR'],
        schema_org_class)

    df_sbert.to_csv(output_dataset_path, sep=';', encoding='utf-8', index=None)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    weight_entities_for_sbert_using_magellan()
