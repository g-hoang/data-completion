import logging
import os
import json

import click
from tqdm import tqdm


@click.command()
@click.option('--schema_org_class')
def deduplicate_training_data(schema_org_class):
    path_to_pretraining = '{}/finetuning/closed_book/{}_finetuning_train.json'.format(os.environ['DATA_DIR'],
                                                                                        schema_org_class)
    path_to_pretraining_dedup = '{}/finetuning/closed_book/{}_finetuning_train_dedup.json'.format(os.environ['DATA_DIR'],
                                                                                      schema_org_class)
    open(path_to_pretraining_dedup, 'w').close()

    record_set = set()
    with open(path_to_pretraining, encoding='utf-8') as file:
        for line in tqdm(file.readlines()):
            record_set.add(line)

    with open(path_to_pretraining_dedup, "a+", encoding='utf-8') as file:
        for entity_str in tqdm(record_set):
            file.write(entity_str)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    deduplicate_training_data()