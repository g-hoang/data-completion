import gzip
import json
import logging
import os
import random

import click
from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.preprocessing.entity_extraction import extract_entity
from src.preprocessing.language_detection import LanguageDetector
from src.strategy.open_book.entity_serialization import EntitySerializer
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.retrieval_strategy import RetrievalStrategy


@click.command()
@click.option('--schema_org_class')
@click.option('--path_to_file')
@click.option('--step_size', help='Number of entities processed at once', type=int, default=10000)
def collect_pretraining_data(schema_org_class, path_to_file, step_size):
    """Load data from ES and generate fine-tuning data"""
    logger = logging.getLogger()

    ld = LanguageDetector()

    # Initialize files for fine-tuning data
    initialize_files_for_pre_training_data(schema_org_class)
    entity_encoder = EntitySerializer(schema_org_class, None)

    counter = 0
    if path_to_file is not None:
        # Load entities from file
        path_to_file = '{}/{}'.format(os.environ['DATA_DIR'], path_to_file)
        with gzip.open(path_to_file, 'rb') as file:
            # 1. Fill index with normalized entities
            raw_entities = [json.loads(line) for line in file.readlines()
                            if 'name' in json.loads(line)
                            and not ld.check_language_is_not_english(json.loads(line)['name'])]
            entities = [extract_entity(raw_entity, schema_org_class) for raw_entity in raw_entities]
            # To-Do: Remove duplicates (?)
            encoded_entities = [entity_encoder.convert_to_str_representation(entity) for entity in entities]
            append_pre_training_data(schema_org_class, encoded_entities)
            counter += len(encoded_entities)


    else:
        strategy = RetrievalStrategy(schema_org_class)
        _es = Elasticsearch([{'host': os.environ['ES_INSTANCE'], 'port': 9200}])
        entity_index_name = determine_es_index_name(schema_org_class)
        no_entities = int(_es.cat.count(entity_index_name, params={"format": "json"})[0]['count'])


        for i in tqdm(range(int(no_entities / step_size) + 1)):
            # Determine retrieval range
            start = i * step_size
            end = start + step_size
            if end > no_entities:
                end = no_entities

            # Retrieve entities & persist them
            entities = strategy.query_tables_index_by_id(range(start, end), entity_index_name)
            entities = [entity['_source'] for entity in entities['hits']['hits']]
            encoded_entities = [entity_encoder.convert_to_str_representation(entity) for entity in entities]
            append_pre_training_data(schema_org_class, encoded_entities)
            counter += len(encoded_entities)

    logger.info('Generated and saved {} entity records for pre-training!'.format(counter))


def initialize_files_for_pre_training_data(schema_org_class):
    """Initialize files for pre-training data"""
    path_to_pretraining = '{}/pretraining/{}_pretraining_train.txt'.format(os.environ['DATA_DIR'], schema_org_class)
    open(path_to_pretraining, 'w').close()
    path_to_pretraining = '{}/pretraining/{}_pretraining_eval.txt'.format(os.environ['DATA_DIR'], schema_org_class)
    open(path_to_pretraining, 'w').close()


def append_pre_training_data(schema_org_class, encoded_entities):
    """Append pre-training data to file"""
    # Split pretraining into train & eval
    path_to_pretraining = '{}/pretraining/{}_pretraining_replace.txt'.format(os.environ['DATA_DIR'],
                                                                             schema_org_class)
    random.shuffle(encoded_entities)
    split_value_1 = int(len(encoded_entities) * 0.8)
    split_value_2 = int(len(encoded_entities) * 0.9)

    with open(path_to_pretraining.replace('replace', 'train'), "a+") as file:
        for entity_str in encoded_entities[:split_value_2]:
            file.write('{}\n'.format(entity_str))

    with open(path_to_pretraining.replace('replace', 'eval'), "a+") as file:
        for entity_str in encoded_entities[split_value_1:]:
            file.write('{}\n'.format(entity_str))

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    collect_pretraining_data()
