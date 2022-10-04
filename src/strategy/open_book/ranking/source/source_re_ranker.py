import logging


def extract_host(table_name):
    """Extract hostname from table name and reverse hostnames
    :param tablename string Input table name
     :return hostname string"""
    table_name_parts = table_name.split('_')
    assert len(table_name_parts) == 3
    host = table_name_parts[1]
    host = '.'.join(reversed(host.split('.')))

    return host


class SourceReRanker:
    def __init__(self, schema_org_class, name):
        self.logger = logging.getLogger()
        self.schema_org_class = schema_org_class
        self.name = name


    def re_rank_evidences(self, query_table, evidences):
        """Re-rank evidences"""
        logger = logging.getLogger()
        logger.warning('Method not implemented!')

        return evidences
