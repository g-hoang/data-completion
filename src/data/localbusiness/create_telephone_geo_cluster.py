import itertools
import logging
import os
import random
import json
import time
from multiprocessing import Pool

from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.preprocessing.value_normalizer import parse_coordinate
from src.similarity.coordinate import haversine
from src.strategy.open_book.es_helper import determine_es_index_name


def calculate_telephone_geo_cluster():
    logger = logging.getLogger()

    es_index = determine_es_index_name('localbusiness')
    elastic_instance = os.environ['ES_INSTANCE']
    _es = Elasticsearch([{'host': elastic_instance, 'port': 9200}])

    # Load Telephone clusters/ Initialize telephone geo clusters
    cluster_summary_file_path = '{}/cluster/localbusiness/telephone_cluster_summary.json'.format(os.environ['DATA_DIR'])
    telephone_clusters = {}
    telephone_geo_clusters = {}
    id_table_assignments = {}
    with open(cluster_summary_file_path, 'r') as csf:
        lines = csf.readlines()

        for line in tqdm(lines):
            cluster = json.loads(line)
            telephone_clusters[cluster['telephone_number']] = cluster['records']

    geo_results = {}
    geo_pool = Pool(20)

    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    for telephone_number in tqdm(telephone_clusters.keys()):
        total_cluster_hits = []
        ids = [record[0] for record in telephone_clusters[telephone_number]]
        no_ids_per_request = 9999
        for subset_ids in chunks(ids, no_ids_per_request):
            query_body = {
                'size': len(subset_ids),
                'query': {
                    'bool': {'must': [
                        {'exists': {'field': 'longitude'}},
                        {'exists': {'field': 'latitude'}},
                        {'terms': {'_id': [str(identifier) for identifier in subset_ids]}}
                    ]
                    }
                }
            }

            # Retrieve entities with phone numbers
            cluster_hits = _es.search(body=json.dumps(query_body), index=es_index)['hits']['hits']
            total_cluster_hits.extend(cluster_hits)

        if len(total_cluster_hits) > 1:
            geo_results[telephone_number] = geo_pool.apply_async(analyse_geo_coordinates, (total_cluster_hits,))
            #telephone_geo_cluster = analyse_geo_coordinates(total_cluster_hits)
            #if len(telephone_geo_cluster) > 0:
            #    telephone_geo_clusters[telephone_number] = analyse_geo_coordinates(total_cluster_hits)
            for hit in total_cluster_hits:
                id_table_assignments[int(hit['_id'])] = hit['_source']['table']


        if len(geo_results) > 0:
            collected_results = []
            for key in geo_results.keys():
                if geo_results[key].ready():
                    telephone_geo_cluster = geo_results[key].get()
                    if len(telephone_geo_cluster) > 0:
                        telephone_geo_clusters[key] = telephone_geo_cluster

                    collected_results.append(key)

            for key in collected_results:
                del geo_results[key]


    logger.info('Wait for all processes to finish!')
    pbar = tqdm(total=len(geo_results))

    while len(geo_results) > 0:
        collected_results = []
        for telephone_number in geo_results.keys():
            if geo_results[telephone_number].ready():
                telephone_geo_cluster = geo_results[telephone_number].get()
                if len(telephone_geo_cluster) > 0:
                    telephone_geo_clusters[telephone_number] = telephone_geo_cluster
                collected_results.append(telephone_number)
                pbar.update(1)
            else:
                logger.info('Waiting for cluster {}!'.format(telephone_number))

        for telephone_number in collected_results:
            del geo_results[telephone_number]

        aggregate_telephone_geo_cluster(telephone_geo_clusters, id_table_assignments)
        time.sleep(60)



    geo_pool.close()
    geo_pool.join()

    pbar.close()

    aggregate_telephone_geo_cluster(telephone_geo_clusters, id_table_assignments)


def analyse_geo_coordinates(cluster_hits):
    # Analyse geo coordinates in addition
    logger = logging.getLogger()
    entities = {}
    for entity in cluster_hits:
        if 'latitude' in entity['_source'] and 'longitude' in entity['_source']:
            try:
                long = parse_coordinate(entity['_source']['longitude'])
                lat = parse_coordinate(entity['_source']['latitude'])
            except ValueError as e:
                logger.debug(e)
                continue

            # Exclude not well maintained geo coordinates
            if lat != 0 and long != 0:
                blocking_key = '{}-{}'.format(round(long, 0), round(lat, 0))
                geo_entity = {'_id': int(entity['_id']), 'long': long, 'lat': lat}
                if blocking_key not in entities:
                    entities[blocking_key] = []

                entities[blocking_key].append(geo_entity)

    telephone_geo_clusters = []
    for blocking_key in entities.keys():
        if len(entities[blocking_key]) > 1:
            # Check if long/lat exactly match before applying expensive haversine distance
            first_lat = None
            first_long = None
            not_all_entities_same_coordinates = False
            for entity in entities[blocking_key]:
                if first_long is None and first_lat is None:
                    first_long = entity['long']
                    first_lat = entity['lat']
                else:
                    if first_long != entity['long'] or first_lat != entity['lat']:
                        not_all_entities_same_coordinates = True
                        break

            if not_all_entities_same_coordinates:
                # Check if entities match using haversine distance
                matches = []
                for entity1, entity2 in itertools.combinations(entities[blocking_key], 2):
                    dis_km = haversine(entity1['long'], entity1['lat'], entity2['long'], entity2['lat'])
                    # Closer than 300 meters
                    if dis_km <= 0.3:
                        matches.append((entity1['_id'], entity2['_id']))

                telephone_geo_clusters.extend(determine_relations(matches))
            else:
                telephone_geo_clusters.append(tuple([entity['_id'] for entity in entities[blocking_key]]))

    # Add Row id & Table to telephone_geo_clusters
    extended_telephone_geo_clusters = []
    for cluster in telephone_geo_clusters:
        extended_cluster = []
        for record_id in cluster:
            for hit in cluster_hits:
                if record_id == int(hit['_id']):
                    extended_record = (hit['_id'], hit['_source']['table'], hit['_source']['row_id'])
                    extended_cluster.append(extended_record)
                    break
        extended_telephone_geo_clusters.append(tuple(extended_cluster))

    return extended_telephone_geo_clusters


def determine_relations(array):

    class Merge:

        def __init__(self,value=None,parent=None,subs=()):
            self.value = value
            self.parent = parent
            self.subs = set()
            # Materialize subs
            for sub in subs:
                for val in sub:
                    self.subs.add(val)
            #self.subs = subs

        def get_ancestor(self):
            cur = self
            while cur.parent is not None:
                cur = cur.parent
            # Introduce short cut to parents (Materialize parents)
            #if self.parent != cur:
            #    self.parent = cur
            return cur

        def __iter__(self):
            if self.value is not None:
                yield self.value
            elif self.subs:
                for val in self.subs:
                    yield val

    random.shuffle(array)
    vals = set(x for tup in array for x in tup)
    dic = {val: Merge(val) for val in vals}
    merge_heads = set(dic.values())

    for frm, to in array:
        mra = dic[frm].get_ancestor()
        mrb = dic[to].get_ancestor()
        mr = Merge(subs=(mra, mrb))
        mra.parent = mr
        mrb.parent = mr
        merge_heads.remove(mra)
        if mrb != mra:
            merge_heads.remove(mrb)
        merge_heads.add(mr)

    return [tuple(set(merge)) for merge in merge_heads]


def aggregate_telephone_geo_cluster(telephone_geo_cluster, id_table_assignments):
    logging.info('Aggregate Telephone Geo Clusters')

    cluster_summary_file_path = '{}/cluster/localbusiness/telephone_geo_cluster_summary.json'.format(os.environ['DATA_DIR'])
    open(cluster_summary_file_path, 'w').close()

    cluster_id = 0
    for telephone_number in telephone_geo_cluster.keys():
        for cluster in telephone_geo_cluster[telephone_number]:
            size = len(cluster)
            if size > 1:
                cluster_id += 1
                ids = [str(record[0]) for record in list(cluster)]
                unique_tables = list(set([id_table_assignments[int(identifier)] for identifier in ids]))
                #for identifier in list(cluster):
                #    del id_table_assignments[identifier]

                cluster = {'cluster_id': cluster_id, 'records': tuple(cluster), 'size': size,
                           'tables': unique_tables, 'no_tables': len(unique_tables)}

                with open(cluster_summary_file_path, 'a') as csf:
                    csf.write(json.dumps(cluster) + '\n')


def retrieve_unique_tables(ids):
    es_index = determine_es_index_name('localbusiness')
    elastic_instance = os.environ['ES_INSTANCE']
    _es = Elasticsearch([{'host': elastic_instance, 'port': 9200}])

    query_body = {
        'size': len(ids),
        'query': {
            'bool': {'must': [
                {'terms': {'_id': [str(identifier) for identifier in ids]}}
            ]
            }
        }
    }

    hits = _es.search(body=json.dumps(query_body), index=es_index)['hits']['hits']
    unique_tables = list(set([hit['_source']['table'] for hit in hits]))
    return unique_tables


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    calculate_telephone_geo_cluster()