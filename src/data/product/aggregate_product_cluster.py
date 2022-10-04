import gzip
import json
import logging
import os
from collections import defaultdict

from tqdm import tqdm


def aggregate_product_clusters():
    logger = logging.getLogger()
    schema_org_class = 'product'

    table_counts = defaultdict(int)
    path_to_lspc_table_corpus_mappings = '{}/cluster/product/lspc2020_to_tablecorpus'.format(os.environ['DATA_DIR'])
    cluster_file_path = '{}_filtered/{}'.format(path_to_lspc_table_corpus_mappings, 'filtered_product_clusters.json.gz')
    try:
        with gzip.open(cluster_file_path, 'rb') as file:
            for line in tqdm(file.readlines()):
                raw_mapping = json.loads(line)
                for record in raw_mapping['records']:
                    table_counts[record['table_id']] += 1

    except gzip.BadGzipFile as e:
        logger.warning('{} - Cannot open file {}'.format(e, cluster_file_path))

    sorted_table_counts = sorted(table_counts.items(), key=lambda k_v: k_v[1], reverse=True)

    for table in sorted_table_counts[:50]:
        print(table)
    #print(sorted_table_counts[:5])



if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    aggregate_product_clusters()