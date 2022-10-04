import logging
from datetime import datetime


from src.model.querytable_new import load_query_tables, load_query_table, get_all_query_table_paths, \
    load_query_table_from_file, get_schema_org_classes, RetrievalQueryTable, get_gt_tables


def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))


# Create a handler for our read (GET) querytables
def read():
    """
    This function responds to a request for /api/querytables
    with the complete lists of querytables

    :return:        list of querytables
    """
    # Create the list of querytables from our data
    querytables = [querytable.to_json(with_evidence_context=True) for querytable in load_query_tables('retrieval') if type(querytable) is RetrievalQueryTable]
    return querytables


# Create a handler for our read (GET) querytables
def get_query_table_by_id(query_table_id):
    """
    This function responds to a request for /api/querytables{queryTableId}
    with a single querytable if it is found

    :return:        searched query table
    """
    # Search for query table
    logger = logging.getLogger()
    for query_table_path in get_all_query_table_paths('retrieval'):
        if query_table_path.endswith('_{}.json'.format(query_table_id)):
            logger.info('Found query table {}!'.format(query_table_id))
            query_table = load_query_table_from_file(query_table_path)
            return query_table.to_json(with_evidence_context=True)

    return None


def update(query_table):
    # logger = logging.getLogger()
    #
    # category = query_table['category'].lower().replace(" ", "_")
    # file_name = 'gs_querytable_{}_{}_{}.json'.format(category, query_table['targetAttribute'], query_table['id'])
    # path_to_query_table = '{}querytables/{}/{}/{}'.format(os.environ['DATA_DIR'], query_table['schemaOrgClass'],
    #                                                       category, file_name)

    query_table = load_query_table(query_table)
    query_table.save(with_evidence_context=False)
    # # Remove context information to keep the size of the gs small
    # for verifiedEvidence in query_table['verifiedEvidences']:
    #     # Remove unnecessary information
    #     if 'context' in verifiedEvidence:
    #         del verifiedEvidence['context']
    #     if 'positiveSignal' in verifiedEvidence:
    #         del verifiedEvidence['positiveSignal']
    #     if 'negativeSignal' in verifiedEvidence:
    #         del verifiedEvidence['negativeSignal']
    #
    # # Refactor and use query table class
    # with open(path_to_query_table, 'w', encoding='utf-8') as f:
    #     json.dump(query_table, f, indent=2)
    #     logger.info('Save query table {}'.format(file_name))


# Create a handler for read (GET) all schema org classes
def get_schema_org_classes_and_categories():
    # Load schema org classes and their categories
    schema_org_classes = []
    for schema_org_class in get_schema_org_classes('retrieval'):
        categories = [' '.join([value.capitalize() for value in category.split('_')])
                      for category in get_gt_tables('retrieval', schema_org_class)]
        schema_org_classes.append({'name': schema_org_class, 'categories': categories})

    return schema_org_classes
