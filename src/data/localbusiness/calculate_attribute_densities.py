import json
import logging
import os

from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.strategy.open_book.es_helper import determine_es_index_name


def calculate_densities():
    logger = logging.getLogger()

    es_index = determine_es_index_name('localbusiness')
    elastic_instance = os.environ['ES_INSTANCE']
    _es = Elasticsearch([{'host': elastic_instance, 'port': 9200}])

    no_entities = int(_es.cat.count(es_index, params={"format": "json"})[0]['count'])
    step_size = 1000

    densities = {}
    unique_tables = set()
    for i in tqdm(range(int(no_entities / step_size) + 1)):

        start = i * step_size
        end = start + step_size
        if end > no_entities:
            end = no_entities

        # Filter ids by already found ids
        ids = [identifier for identifier in range(start, end)]
        if len(ids) > 0:
            logger.info('Will look for {} entities by id'.format(len(ids)))
            query_body = {
                'size': len(ids),
                'query': {
                    'bool': {'must': [
                        {'terms': {'_id': [str(identifier) for identifier in ids]}}
                    ]
                    }
                }
            }

            # Retrieve entities with phone numbers
            result = _es.search(body=json.dumps(query_body), index=es_index)

            for hit in result['hits']['hits']:
                for key in hit['_source']:
                    if key not in densities:
                        densities[key] = 0
                    densities[key] += 1

                unique_tables.add(hit['_source']['table'])

    total_no_filled_cells = 0
    for key in densities:
        total_no_filled_cells += densities[key]
        attribute_density = densities[key] / no_entities
        logger.info('Density of {}: {}'.format(key, attribute_density))

    total_density = total_no_filled_cells / (len(densities.keys()) * no_entities)
    logger.info('Total density: {}'.format(total_density))

    logger.info('Number of unique tables: {}'.format(len(list(unique_tables))))

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    calculate_densities()