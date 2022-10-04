import logging

from src.strategy.open_book.ranking.source.page_rank_re_ranker import PageRankReRanker
from src.strategy.open_book.ranking.source.source_re_ranker import SourceReRanker


def select_source_re_ranker(re_ranking_strategy, schema_org_class):
    logger = logging.getLogger()

    if re_ranking_strategy is None:
        re_ranking_strategy_name = None
    else:
        re_ranking_strategy_name = re_ranking_strategy['name']

    logger.info('Select Source Re-ranking Strategy {}!'.format(re_ranking_strategy_name))

    if re_ranking_strategy_name == 'page_rank_re_ranker':
        re_ranking_strategy = PageRankReRanker(schema_org_class)
    elif re_ranking_strategy_name is None:
        # Do not supply a re-ranking strategy
        re_ranking_strategy = None
    else:
        # Fall back to default reranker, which does no re-ranking
        re_ranking_strategy = SourceReRanker(schema_org_class, None)

    return re_ranking_strategy
