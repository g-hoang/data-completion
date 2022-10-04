import os

import pandas as pd

from src.strategy.open_book.ranking.source.source_re_ranker import SourceReRanker, extract_host


class PageRankReRanker(SourceReRanker):
    def __init__(self, schema_org_class):
        super().__init__(schema_org_class, 'Page Rank ReRanker')
        self.host_page_rank = self.load_source_rankings()

    def load_source_rankings(self):
        self.logger.info('Load Source Page Ranks!')
        file_path = '{}/ranking/cc-main-2020-jul-aug-sep-relevant-tc-domain-page-ranks-{}.txt' \
            .format(os.environ['DATA_DIR'], self.schema_org_class)

        df_page_ranks = pd.read_csv(file_path, sep='\t')
        host_page_rank = dict(zip(df_page_ranks['reserved hostname'], df_page_ranks['rescaled page rank']))
        #print(df_page_ranks['rescaled page rank'].head())

        return host_page_rank

    def re_rank_evidences(self, query_table, evidences):
        """Re-rank evidences"""

        # Look page rank up for each evidence
        for evidence in evidences:
            host = extract_host(evidence.table)
            page_rank = 0
            if host in self.host_page_rank:
                page_rank = self.host_page_rank[host]
            evidence.scores[self.name] = page_rank
            evidence.similarity_score = page_rank

        return evidences
