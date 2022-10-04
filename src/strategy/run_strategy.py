#!/usr/bin/env python3

import logging
import time
from datetime import datetime
from multiprocessing import Pool

import click
import yaml
from tqdm import tqdm

from src.evaluation.evaluate_query_tables import evaluate_query_table
from src.model.querytable_new import load_query_table_from_file, get_gt_tables, get_query_table_paths
from src.strategy.open_book.ranking.similarity.similarity_re_ranking_factory import select_similarity_re_ranker
from src.strategy.open_book.ranking.source.source_re_ranking_factory import select_source_re_ranker
from src.strategy.open_book.retrieval.retrieval_strategy_factory import select_retrieval_strategy
from src.strategy.pipeline_building import build_pipelines_from_configuration, validate_configuration


@click.command()
@click.option('--path_to_config')
@click.option('--worker', type=int, default=0)

def run_experiments_from_configuration(path_to_config, worker):
    logger = logging.getLogger()

    # Load yaml configuration
    with open(path_to_config) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    validate_configuration(config)

    context_attributes = config['query-tables']['context-attributes']
    experiment_type = config['general']['experiment-type']

    # Load query tables
    schema_org_class = config['query-tables']['schema_org_class']
    query_table_paths = []
    if type(config['query-tables']['path-to-query-table']) is str:
        # Run for single query table
        query_table_paths.append(config['query-tables']['path-to-query-table']) # query_table_paths must be an array
    elif config['query-tables']['gt-table'] is not None:
        # Run on query tables for gt table
        query_table_paths.extend(get_query_table_paths(config['general']['experiment-type'],
                                                  config['query-tables']['schema_org_class'],
                                                  config['query-tables']['gt-table']))
    else:
        # Run on all query tables of schema org class
        for gt_table in get_gt_tables(config['general']['experiment-type'], schema_org_class):
            query_table_paths.extend(get_query_table_paths(config['general']['experiment-type'], schema_org_class,
                                                           gt_table))

    string_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    evidence_count = config['general']['evidence_count']
    save_results_with_evidences = config['general']['save_results_with_evidences']
    clusters = config['general']['clusters']
    #os.environ["ES_INSTANCE"] = config['general']['es_instance']

    pool = None
    async_results = None
    if worker > 0:
        pool = Pool(worker)
        async_results = []

    file_name = 'results_{}.json'.format(string_timestamp)

    # Build pipelines from yaml configuration
    pipelines = build_pipelines_from_configuration(config)

    # Start run experiments by combining pipelines and query tables
    for pipeline in pipelines:
        retrieval_strategy = pipeline['retrieval_strategy']
        similarity_re_ranking_strategy = pipeline['similarity_re_ranking_strategy']
        source_re_ranking_strategy = pipeline['source_re_ranking_strategy']
        voting_strategies = pipeline['voting_strategies']

        if worker == 0:
            results = run_experiments(experiment_type, retrieval_strategy, similarity_re_ranking_strategy,
                                      source_re_ranking_strategy,
                                      voting_strategies, query_table_paths, schema_org_class, evidence_count,
                                      context_attributes, clusters=clusters)
            if results is not None:
                for result in results:
                    result.save_result(file_name, save_results_with_evidences)
        elif worker > 0:
            async_results.append(pool.apply_async(run_experiments, (experiment_type, retrieval_strategy,
                                                                    similarity_re_ranking_strategy,
                                                                    source_re_ranking_strategy, voting_strategies,
                                                                    query_table_paths, schema_org_class, evidence_count,
                                                                    context_attributes, clusters)))

    if worker > 0:
        logger.info('Waiting for all experiments to finish!')

        while len(async_results) > 0:
            logger.info('Number of chunks: {}'.format(len(async_results)))
            time.sleep(5)
            async_results = collect_results_of_finished_experiments(async_results, file_name,
                                                                    save_results_with_evidences)

        logger.info('Finished running experiments!')


def run_experiments(experiment_type, retrieval_str_conf, similarity_re_ranking_str_conf, source_re_ranking_str_conf,
                    voting_strategies, query_table_paths, schema_org_class, evidence_count, context_attributes=None, clusters=False):
    """Run Pipeline on query tables"""

    logger = logging.getLogger()
    # Initialize strategy
    retrieval_strategy = select_retrieval_strategy(retrieval_str_conf, schema_org_class, clusters)

    #similarity_re_ranker = select_similarity_re_ranker(similarity_re_ranking_str_conf, schema_org_class,
    #                                                   context_attributes)
    source_re_ranker = select_source_re_ranker(source_re_ranking_str_conf, schema_org_class)
    logger.info('Run experiments on {} query tables'.format(len(query_table_paths)))
    results = []

    for query_table_path in tqdm(query_table_paths):

        query_table = load_query_table_from_file(query_table_path)
        similarity_re_ranker = select_similarity_re_ranker(similarity_re_ranking_str_conf, schema_org_class, context_attributes)
        # FIX context attributes
        if experiment_type == 'augmentation' and context_attributes is not None:
            if query_table.target_attribute in query_table.context_attributes:
                continue
            # Run experiments only on a subset of context attributes
            removable_attributes = [attr for attr in query_table.context_attributes
                                    if attr not in context_attributes and attr != 'name']
            for attr in removable_attributes:
                query_table.remove_context_attribute(attr)

        evidences = retrieve_evidences_with_pipeline(query_table,retrieval_strategy, evidence_count,
                                                     similarity_re_ranker, source_re_ranker, entity_id=None)
        logging.info(f'Number of evidences: {len(evidences)}')

        if retrieval_str_conf['name'] == 'generate_entity':
            k_intervals = [5]
        else:
            #k_intervals = [1, 5, 10, 20, 50, evidence_count]
            k_intervals = [1, 2, 5, 10, 20, 30, 50]

        for voting_str_conf in voting_strategies:
            new_results = evaluate_query_table(query_table, experiment_type, retrieval_strategy, similarity_re_ranker,
                                               source_re_ranker, evidences, k_intervals, voting_str_conf['name'])
            results.extend(new_results)
        # else:
        #     k_intervals = [5, 10, 20, 50, 70]
        #     #k_intervals = [5, 10]
        #
        #     for voting_str_conf in voting_strategies:
        #         new_results = evaluate_query_table(query_table, retrieval_strategy, similarity_re_ranker, source_re_ranker,
        #                                        evidences, k_intervals, voting_str_conf['name'])
        #         results.extend(new_results)

    logger.info('Finished running experiments on subset of query tables!')
    return results


def retrieve_evidences_with_pipeline(query_table, retrieval_strategy, evidence_count,
                                     similarity_re_ranker, source_re_ranker, entity_id=None, data_type='origin'):
    logger = logging.getLogger()
    # Run retrieval strategy
    evidences = retrieval_strategy.retrieve_evidence(query_table, evidence_count, entity_id)

    # Filter evidences by ground truth tables
    evidences = retrieval_strategy.filter_evidences_by_ground_truth_tables(evidences)

    # Run re-ranker
    if similarity_re_ranker is not None:
        # Re-rank evidences by cross encoder - to-do: Does it make sense to track both bi encoder and reranker?
        evidences = similarity_re_ranker.re_rank_evidences(query_table, evidences)

    if source_re_ranker is not None:
        # Re-rank evidences by source information
        evidences = source_re_ranker.re_rank_evidences(query_table, evidences)

    return evidences


def collect_results_of_finished_experiments(async_results, file_name, with_evidences=False):
    """Collect results and write them to file"""
    logger = logging.getLogger()
    collected_results = []
    for async_result in async_results:
        if async_result.ready():
            results = async_result.get()
            collected_results.append(async_result)

            # Save query table to file
            if results is not None:
                logger.info('Will collect {} results now!'.format(len(results)))
                for result in results:
                    result.save_result(file_name, with_evidences)

    # Remove collected results from list of results
    async_results = [async_result for async_result in async_results if async_result not in collected_results]

    return async_results


def run_strategy_to_retrieve_evidence(query_table_id, schema_org_class, experiment_type, retrieval_str_conf,
                                      similarity_re_ranking_str_conf, source_re_ranking_str_conf, entity_id=None):
    # TO-DO: UPDATE SO THAT THE ANNOTATION TOOL CONTINUES TO WORK!
    # Initialize Table Augmentation Strategy
    evidence_count = 30  # Deliver 20 evidence records for now

    print(retrieval_str_conf)
    #To-Do: Does it make sense to set clusters always to true?
    retrieval_strategy = select_retrieval_strategy(retrieval_str_conf, schema_org_class, clusters=True)
    similarity_re_ranker = select_similarity_re_ranker(similarity_re_ranking_str_conf, schema_org_class)
    source_re_ranker = select_source_re_ranker(source_re_ranking_str_conf, schema_org_class)

    query_table = None
    context_attributes = ['name', 'addresslocality']

    for gt_table in get_gt_tables(experiment_type, schema_org_class):
        for query_table_path in get_query_table_paths('retrieval', schema_org_class, gt_table):
            if query_table_path.endswith('_{}.json'.format(query_table_id)):
                query_table = load_query_table_from_file(query_table_path)
                # Run experiments only on a subset of context attributes
                removable_attributes = [attr for attr in query_table.context_attributes
                                        if attr not in context_attributes and attr != 'name']
                for attr in removable_attributes:
                    query_table.remove_context_attribute(attr)

    evidences = retrieve_evidences_with_pipeline(query_table, retrieval_strategy, evidence_count,
                                                 similarity_re_ranker, source_re_ranker, entity_id=entity_id)

    # Return evidence --> (Filter for single entity)
    requested_evidence = [evidence for evidence in evidences
                          if evidence.entity_id == entity_id]

    return requested_evidence[:evidence_count]


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    run_experiments_from_configuration()
