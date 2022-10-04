import logging
import os

import click
import pandas as pd

from src.strategy.open_book.entity_serialization import EntitySerializer


@click.command()
@click.option('--schema_org_class')
@click.option('--training_method', default='sbert')
def load_and_serialize_entities(schema_org_class, training_method):

    dataset_path = '{}/finetuning/open_book/{}_fine-tuning_extended_subset_pairs.csv'.format(os.environ['DATA_DIR'],
                                                                                 schema_org_class)


    df_dataset = pd.read_csv(dataset_path, sep=';', encoding='utf-8')
    serialize_entities(schema_org_class, df_dataset, training_method)


def serialize_entities(schema_org_class, df_dataset, training_method):

    logger = logging.getLogger()
    if schema_org_class == 'localbusiness':
        context_attributes = ['name', 'addresslocality']
    elif schema_org_class == 'product':
        context_attributes = ['name']
    else:
        ValueError('Schema Org Class {} is unknown'.format(schema_org_class))

    df_dataset['entity1'] = df_dataset.apply(lambda row: serialize_entity(row, 'entity1', schema_org_class,
                                                                          context_attributes), axis=1)
    df_dataset['entity2'] = df_dataset.apply(lambda row: serialize_entity(row, 'entity2', schema_org_class,
                                                                          context_attributes), axis=1)

    output_dataset_path = '{}/finetuning/open_book/{}_fine-tuning_{}_extended_subset_pairs.csv'.format(os.environ['DATA_DIR'],
                                                                                 schema_org_class, training_method)

    if schema_org_class == 'localbusiness':
        df_dataset = df_dataset.drop(columns=['entity1_name', 'entity1_addresslocality',
                                              'entity2_name', 'entity2_addresslocality'], axis=1)
    elif schema_org_class == 'product':
        df_dataset = df_dataset.drop(columns=['entity1_name', 'entity2_name'], axis=1)
    else:
        ValueError('Schema Org Class {} is unknown'.format(schema_org_class))

    df_dataset = df_dataset.loc[~((df_dataset['score'] == 0) & (df_dataset['entity1'] == df_dataset['entity2']))]
    df_dataset = df_dataset.drop_duplicates()
    if training_method == 'cross':
        df_dataset['entities'] = df_dataset['entity1']
    df_dataset.to_csv(output_dataset_path,  sep=';', encoding='utf-8', index=False)
    logger.info('{} finetuning data saved'.format(training_method))


def serialize_entity(row, entity_name, schema_org_class, context_attributes):
    entity_serializer = EntitySerializer(schema_org_class, context_attributes)

    entity = {attr.replace(entity_name + '_', ''): value for attr, value in row.items() if entity_name in attr}
    serialized_entity = entity_serializer.convert_to_str_representation(entity)
    return serialized_entity

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=log_fmt,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    serialize_entities()