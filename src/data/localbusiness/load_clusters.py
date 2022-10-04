import json
import os

from tqdm import tqdm


def load_clusters(path):
    cluster_summary_file_path = '{}/cluster/{}'.format(os.environ['DATA_DIR'], path)

    clusters = []
    with open(cluster_summary_file_path, 'r') as csf:
        lines = csf.readlines()

        for line in tqdm(lines):
            clusters.append(json.loads(line))

    return clusters