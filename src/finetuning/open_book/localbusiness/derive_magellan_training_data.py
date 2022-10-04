import logging
import os

import click
import pandas as pd

@click.command()
@click.option('--schema_org_class')
def derive_magellan_training_data(schema_org_class):
    """Use Training data of cross encoder to derive magellan training data.
        DO NOT FURTHER USE THIS SCRIPT!
        It is used as a short cut to not reproduce all training data, train models and index data"""
    path_to_fine_tuning_split = '{}/finetuning/open_book/{}_fine-tuning_cross_encoder_new.csv'.format(os.environ['DATA_DIR'],
                                                                                       schema_org_class)
    df_magellan = pd.read_csv(path_to_fine_tuning_split, sep=';', encoding='utf-8')

    df_magellan[['entity1', 'entity2']] = df_magellan['entities'].str.split('\\[SEP\\]', -1, expand=True)
    df_entity1 = deserialize_entity(df_magellan['entity1'], 'entity1')
    df_entity2 = deserialize_entity(df_magellan['entity2'], 'entity2')

    df_magellan = pd.concat([df_magellan, df_entity1, df_entity2], axis=1)

    path_to_fine_tuning_split = '{}/finetuning/open_book/{}_fine-tuning_magellan_new.csv'.format(os.environ['DATA_DIR'],
                                                                                       schema_org_class)
    df_magellan.drop(columns=['entities', 'entity1', 'entity2'], inplace=True)

    df_magellan.to_csv(path_to_fine_tuning_split, sep=';', encoding='utf-8', index=False, float_format='%.0f')
    logging.info('Magellan finetuning data saved')


def deserialize_entity(series, entity):

    df_entity = pd.DataFrame()
    splits = series.str.split('\\[COL\\]', -1, expand=True)
    for split in splits.columns:
        df_attr_value = pd.DataFrame()
        if split == 0:
            # First column is empty!
            continue
        df_attr_value[['attribute', 'value']] = splits[split].str.split('\\[VAL\\]', -1, expand=True)
        unique_values = [value for value in df_attr_value['attribute'].unique() if value is not None]

        if len(unique_values) == 1:
            attribute_name = unique_values[0]
            del df_attr_value['attribute']
            df_attr_value.columns = ['{}_{}'.format(entity, attribute_name)]

            df_entity = pd.concat([df_entity, df_attr_value], axis=1)
        elif len(unique_values) > 1:
            logging.warning('Multiple attribute names found: {}'.format(unique_values))

    return df_entity



if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    derive_magellan_training_data()
