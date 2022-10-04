import gzip
import json
import logging

from tqdm import tqdm


def load_clusters(path, table):
    """Load cluster
        :param path string Path to Clusters
        :param table string ground truth table for which the query tables are assembled"""
    logger = logging.getLogger()
    logger.info('Load Clusters')
    clusters = []

    try:
        with gzip.open(path, 'rb') as file:
        #with open(path, 'r') as file:
            for line in tqdm(file.readlines()):
                cluster = json.loads(line)
                cluster['tables'] = list(set([record['table_id'] for record in cluster['records']]))
                if table is not None:
                    if table in cluster['tables']:
                        clusters.append(cluster)
                else:
                    clusters.append(cluster)

    except gzip.BadGzipFile as e:
        logger.warning('{} - Cannot open file {}'.format(e, path))

    return clusters
