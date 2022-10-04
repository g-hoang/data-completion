import gzip
import json
import logging
import os
import random
import yaml
import re
import statistics
import numpy as np
import pandas as pd

import click
from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.preprocessing.entity_extraction import extract_entity
from src.preprocessing.language_detection import LanguageDetector
from src.strategy.closed_book.generate_target_attribute_value import create_source_sequence2, create_natural_question
from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.retrieval_strategy import RetrievalStrategy
from src.strategy.pipeline_building import validate_configuration


@click.command()
@click.option('--path_to_config')
@click.option('--path_to_file')
@click.option('--step_size', help='Number of entities processed at once', type=int, default=10000)
def load_data(path_to_config, path_to_file, step_size):
    """Load data from ES and generate fine-tuning data"""
    logger = logging.getLogger()
    ld = LanguageDetector()
    with open(path_to_config) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    validate_configuration(config)

    attributes = {
        'context_attributes': config['query-tables']['context-attributes'],
        'target_attributes': config['query-tables']['target-attributes']
    }
    schema_org_class = config['query-tables']['schema_org_class']
    generate_option = config['general']['training_data_type']

    # Initialize files for fine-tuning data
    path_to_pretraining = '{}/fine-tuning/closed_book/{}_finetuning_train.json'.format(os.environ['DATA_DIR'],
                                                                                      schema_org_class)
    open(path_to_pretraining, 'w').close()
    # path_to_pretraining = '{}/fine-tuning/closed_book/{}_finetuning_eval.json'.format(os.environ['DATA_DIR'],
    #                                                                                  schema_org_class)
    # open(path_to_pretraining, 'w').close()

    # Dict for tracking country - locality (CL) & country - region (CR) & locality - region (LR)
    frequencies = {'CL': {}, 'CR': {}, 'LR': {}, 'LC': {}, 'RC': {}, 'RL': {}}
    count_record_rejected = 0
    counter = 0
    table_counter = {}

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
            entities = cleaning(entities, schema_org_class)
            peristed_records, table_counter = persist_data_set_records(schema_org_class, entities, frequencies, count_record_rejected,
                                                  attributes, generate_option, table_counter)
            counter += peristed_records


    else:
        strategy = RetrievalStrategy(schema_org_class, 'generate_entity')
        _es = Elasticsearch([{'host': os.environ['ES_INSTANCE'], 'port': 9200}])
        entity_index_name = determine_es_index_name(schema_org_class)
        no_entities = int(_es.cat.count(entity_index_name, params={"format": "json"})[0]['count'])

        for i in tqdm(range(int(no_entities / step_size) + 1)):
            # Determine retrieval range
            start = i * step_size
            end = start + step_size
            if end > no_entities:
                end = no_entities

            # Retrieve entities, exclude entities from ground truth tables & persist them
            entities = strategy.query_tables_index_by_id(range(start, end), entity_index_name)
            entities = [entity['_source'] for entity in entities['hits']['hits']
                        if not ld.check_language_is_not_english(entity['_source']['name'])
                        # if entity['_source']['table'] not in strategy.ground_truth_tables[schema_org_class]
                       ]
            entities = cleaning(entities, schema_org_class)
            peristed_records, table_counter = persist_data_set_records(schema_org_class, entities, frequencies, count_record_rejected,
                                                  attributes, generate_option, table_counter)
            counter += peristed_records

    logger.info('Generated and saved {} data set records!'.format(counter))
    logger.info('Rejected the creation of {} records!'.format(step_size - peristed_records))

# Remove low-quality entities
def cleaning(entities, schema_org_class):
    logger = logging.getLogger()

    def clean_duration(entities):
        cleaned_entities = []
        for entity in entities:
            if 'duration' in entity:
                x = re.search("^PT(\d{1,2}[H,M,S,h,m,s])+$", entity['duration'])
                if not x:
                    logger.info(f"Duration is not valid: {entity['duration']}")
                    del entity['duration']
            cleaned_entities.append(entity)
        return cleaned_entities
    
    def clean_datepublished(entities):
        cleaned_entities = []
        for entity in entities:
            # Assume the year in name is the correct year of publication
            yearPublished = None
            if 'name' in entity:
                year = re.search("((18|19|20)\d{2})", entity['name'])
                if year:
                    yearPublished = year.group()
            if 'datepublished' in entity:
                correctFormat = re.search("^\d{4}-\d{2}-\d{2}$", entity['datepublished'])
                if not correctFormat or (yearPublished and yearPublished != entity['datepublished'][:4]):
                    logger.info(f"datepublished is not valid: {entity['datepublished']} for entity {entity['name']}")
                    del entity['datepublished']

            cleaned_entities.append(entity)
        return cleaned_entities
    
    def clean_address(entities):
        cleaned_entities = []
        for entity in entities:
            if 'addresscountry' in entity and len(entity['addresscountry']) > 2:
                logger.info(f"Addresscountry is not valid: {entity['addresscountry']}")
                del entity['addresscountry']
            cleaned_entities.append(entity)
        return cleaned_entities
    
    def clean_postalcode(entities):
        cleaned_entities = []
        for entity in entities:
            if 'postalcode' in entity:
                if entity['postalcode'][0] == ',':
                    entity['postalcode'] = entity['postalcode'].replace(',', '').strip()
                if len(entity['postalcode']) > 10 or len(entity['postalcode']) < 5:
                    logger.info(f"Postalcode is not valid: {entity['postalcode']}")
                    del entity['postalcode']
            cleaned_entities.append(entity)
        return cleaned_entities
    
    def clean_telephone(entities):
        cleaned_entities = []
        for entity in entities:
            if 'telephone' in entity:
                if entity['telephone'][0] != '+':
                    entity['telephone'] = '+' + entity['telephone']
                if len(entity['telephone']) < 8:
                    logger.info(f"Telephone is not valid: {entity['telephone']}")
                    del entity['telephone']
            cleaned_entities.append(entity)
        return cleaned_entities
    
    if schema_org_class == 'movie':
        entities = clean_duration(entities)
        entities = clean_datepublished(entities)
    elif schema_org_class == 'localbusiness':
        entities = clean_address(entities)
        entities = clean_postalcode(entities)
        entities = clean_telephone(entities)
    
    return entities

def generate_finetuning_seq2seq_data(entities, frequencies, count_record_rejected, attributes, table_counter):
    """Generate fine-tuning data"""

    # Select target/ identifying attributes
    # if schema_org_class == 'movie':
    #     target_attributes = ['director', 'duration', 'datepublished', 'isbasedon']
    # elif schema_org_class == 'localbusiness':
    #     # Exclude addresslocality for now
    #     #target_attributes = ['addresslocality', 'addressregion', 'addresscountry', 'postalcode', 'streetaddress', 'telephone']
    #     target_attributes = ['addressregion', 'addresscountry', 'postalcode', 'streetaddress', 'telephone']
    # else:
    #     logging.warning(
    #         'Target/Identifying attributes are not defined for schema org class {}'.format(schema_org_class))

    target_attributes = attributes['target_attributes']

    finetuning_records = []
    for entity in entities:
        if entity['table'] not in table_counter:
            table_counter[entity['table']] = 1
        else:
            table_counter[entity['table']] += 1

        for target_attribute in target_attributes:
            if target_attribute in entity:

                # Check frequency combinations of addresscountry, addresslocality & addressregion - Decreased performance!
                create_record = True
                #if target_attribute in ['addresslocality', 'addressregion', 'addresscountry']:
                #    create_record = check_frequencies(entity, target_attribute, frequencies)
                #    if not create_record:
                #        logging.info('Record not created for target attribute value: {}'.format(entity[target_attribute]))

                if create_record:
                    source = create_source_sequence2(entity, target_attribute, attributes['context_attributes'])
                    target = "[VAL]{}".format(entity[target_attribute])
                    record = {"table_augmentation": {"source": source, "target": target}}
                    finetuning_records.append(record)
                else:
                    count_record_rejected += 1

    return finetuning_records, table_counter

def check_frequencies(entity, target_attribute, frequencies):
    relations = {'addresscountry': ('addressregion', 'addresslocality'),
                 'addresslocality': ('addresscountry', 'addressregion'),
                 'addressregion': ('addresscountry', 'addresslocality')}
    frequency_keys = {'addresscountry': {'addressregion': 'CR', 'addresslocality': 'CL'},
                      'addressregion': {'addresscountry': 'RC', 'addresslocality': 'RL'},
                      'addresslocality': {'addresscountry': 'LC', 'addressregion': 'LR'}}

    if target_attribute in entity:
        # Check if frequency record exists
        c_attr_1 = relations[target_attribute][0]
        c_attr_2 = relations[target_attribute][1]
        if c_attr_1 in entity:
            frequency_key_1 = frequency_keys[target_attribute][c_attr_1]
            if not entity[target_attribute] in frequencies[frequency_key_1]:
                frequencies[frequency_key_1][entity[target_attribute]] = {}

            if not entity[c_attr_1] in frequencies[frequency_key_1][entity[target_attribute]]:
                frequencies[frequency_key_1][entity[target_attribute]][entity[c_attr_1]] = 0

        if c_attr_2 in entity:
            frequency_key_2 = frequency_keys[target_attribute][c_attr_2]
            if not entity[target_attribute] in frequencies[frequency_key_2]:
                frequencies[frequency_key_2][entity[target_attribute]] = {}

            if not entity[c_attr_2] in frequencies[frequency_key_2][entity[target_attribute]]:
                frequencies[frequency_key_2][entity[target_attribute]][entity[c_attr_2]] = 0

        if c_attr_1 in entity and c_attr_2 in entity:
            frequency_key_1 = frequency_keys[target_attribute][c_attr_1]
            frequency_key_2 = frequency_keys[target_attribute][c_attr_2]
            frequencies[frequency_key_1][entity[target_attribute]][entity[c_attr_1]] += 1
            frequencies[frequency_key_2][entity[target_attribute]][entity[c_attr_2]] += 1
            return frequencies[frequency_key_1][entity[target_attribute]][entity[c_attr_1]] < 100 \
                   and frequencies[frequency_key_2][entity[target_attribute]][entity[c_attr_2]] < 100

        elif c_attr_1 in entity:
            frequency_key_1 = frequency_keys[target_attribute][c_attr_1]
            frequencies[frequency_key_1][entity[target_attribute]][entity[c_attr_1]] += 1
            return frequencies[frequency_key_1][entity[target_attribute]][entity[c_attr_1]] < 100

        elif c_attr_2 in entity:
            frequency_key_2 = frequency_keys[target_attribute][c_attr_2]
            frequencies[frequency_key_2][entity[target_attribute]][entity[c_attr_2]] += 1
            return frequencies[frequency_key_2][entity[target_attribute]][entity[c_attr_2]] < 100

def generate_natural_questions_set(entities, frequencies, attributes):
    # Generate natural language question
    # Who be director of Harry Potter with duration be PT2H32M and datepublished be 2001-11-16 (for human)
    # When be datepublished of Harry Potter with duration be PT2H32M and director be Chris Columbus (for datetime format)
    # Where be addresslocality of Hyatt Paris Madeleine with telephone be +33155271234 and postalcode be 75008 (For location features)
    # How be duration of Harry Potter with director be Chris Columbus and datepublished be 2001-11-16 (for duration or ratings?)
    # What be telephone of Hyatt Paris Madeleine with addresslocality be Paris and postalcode be 75008 and streetaddress be 24 Boulevard Malesherbes, 75008 (for others)

    finetuning_records = []
    for target_attribute in attributes['target_attributes']:
        for entity in entities:
            if target_attribute in entity:
                source = create_natural_question(entity, target_attribute, attributes['context_attributes'])
                target = entity[target_attribute]
                record = {"table_augmentation": {"source": source, "target": target}}
                finetuning_records.append(record)

    return finetuning_records

def persist_data_set_records(schema_org_class, entities, frequencies, count_record_rejected, attributes, generate_option, table_counter):
    """Split data set records into train/validation and persist both data sets"""
    if generate_option == 'qa':
        dataset_records = generate_natural_questions_set(entities, frequencies, attributes)
    else:
        # Default case, generate linearized sequence
        dataset_records, table_counter = generate_finetuning_seq2seq_data(entities, frequencies, count_record_rejected, attributes, table_counter)

    path_to_pretraining = '{}/fine-tuning/closed_book/{}/{}_finetuning_train.json'.format(os.environ['DATA_DIR'], generate_option,
                                                                                        schema_org_class)
    random.shuffle(dataset_records)

    with open(path_to_pretraining, "a+", encoding='utf-8') as file:
        for entity_str in dataset_records:
            file.write('{}\n'.format(json.dumps(entity_str)))

    return len(dataset_records), table_counter

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    load_data()
