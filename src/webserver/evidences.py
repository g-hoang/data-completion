from datetime import datetime

from src.strategy import run_strategy


def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))


# Create a handler for our read (GET) querytables
def findForEntity(query_table_id, entity_id, schema_org_class, strategy):
    """
    This function responds to a request for /api/evidence/findForEntity
    with all found evidences for the requested entity

    :return:        searched query table
    """

    # Data to serve with our API
    #with open('mock-evidence.json') as gsFile:
    #    EVIDENCES = json.load(gsFile)

    # Search for query table
    #return [evidence for evidence in EVIDENCES if evidence['queryTableId'] == query_table_id and evidence['entityId'] == entity_id]
    #model_name = 'finetuned_movie_t5-v1_1-small'
    #model_name = 'finetuned_sbert_bert-base-uncased_mean_{}_new'.format(schema_org_class)
    model_name = 'finetuned_sbert_bert-base-uncased_mean_localbusiness_weight_corner_cases_missing_values_2'

    pooling = 'mean'
    similarity = 'cos'
    print(strategy)
    retrieval_str_conf = {'name': strategy, 'bi-encoder': 'huggingface_bi_encoder', 'model_name': model_name,
                          'pooling': pooling, 'similarity': similarity}
    reranking_str_conf = {'name': 'symbolic_re_ranker', 'similarity_measure': 'jaccard'}

    evidences = run_strategy.run_strategy_to_retrieve_evidence(query_table_id, schema_org_class, 'retrieval',
                                                               retrieval_str_conf, reranking_str_conf, None, entity_id=entity_id)

    for evidence in evidences:
        evidence.aggregate_scores_to_similarity_score()

    evidences.sort(key=lambda evidence: evidence.similarity_score, reverse=True)

    #return run_strategy.run_strategy_to_retrieve_evidence(query_table_id, entity_id, schema_org_class, 'elastic_by_entity_from_multiple_tables')

    # Encode evidences
    evidences = [evidence.to_json(with_evidence_context=True, without_score=False) for evidence in evidences]

    return evidences