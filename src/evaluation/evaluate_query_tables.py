import logging

from src.model.result import Result
from src.preprocessing.value_normalizer import get_datatype, normalize_value
from src.similarity.coordinate import haversine


def evaluate_query_table(query_table, experiment_type, retrieval_strategy, similarity_reranker, source_reranker,
                         retrieved_evidences, k_interval, voting='weighted'):
    """
    Calculate mean precision and recall for list of provided evidences
    :param  Querytable query_table: Query Table
    :param  experiment_type String: type of experiments (retrieval/ augmentation)
    :param  RetrievalStrategy strategy_obj: Retrieval strategy
    :param  list[evidences] retrieved_evidences: List of found evidences
    :param  list[integer]  k_interval: Interval at which the retrieved list of evidences is evaluated
    :param  String   voting: voting strategy- simple (value majority) or weighted (similarity scores)
    """
    logger = logging.getLogger()
    logger.info('Evaluate query table {}: {}'.format(query_table.identifier, query_table.assembling_strategy))

    ranking_lvls = ['3 - Correct Value and Entity', '3,2 - Relevant Value and Correct Entity', '3,2,1 - Correct Entity']
    ranking_lvls = ['3,2,1 - Correct Entity'] # Fix ranking lvl for now! (Reduce number of combinations)
    results = []

    # Aggregate different scores of evidences to final similarity score
    for evidence in retrieved_evidences:
        evidence.aggregate_scores_to_similarity_score()

    for ranking_lvl in ranking_lvls:
        result = Result(query_table, retrieval_strategy, similarity_reranker, source_reranker, k_interval, ranking_lvl,
                        voting)

        if not query_table.has_verified_evidences():
            logger.warning('No verified evidences found for query table {}!'.format(query_table.identifier))
            return [ result ]

        if experiment_type == 'augmentation':
            if ranking_lvl == '3 - Correct Value and Entity':
                relevance_classification = [3]
            elif ranking_lvl == '3,2 - Relevant Value and Correct Entity':
                relevance_classification = [3, 2]
            else:
                relevance_classification = [3, 2, 1]

            positive_evidences = [evidence for evidence in query_table.verified_evidences
                                 if evidence.scale in relevance_classification]
            negative_evidences = [evidence for evidence in query_table.verified_evidences
                                 if evidence.scale not in relevance_classification]

            # Filter evidences - Remove ground truth tables
            positive_evidences = retrieval_strategy.filter_evidences_by_ground_truth_tables(positive_evidences)
            negative_evidences = retrieval_strategy.filter_evidences_by_ground_truth_tables(negative_evidences)
        else:
            positive_evidences = retrieval_strategy.filter_evidences_by_ground_truth_tables(query_table.verified_evidences)
            negative_evidences = []

        for row in query_table.table:
            all_rel_retrieved_evidences = [evidence for evidence in retrieved_evidences if
                                           evidence.entity_id == row['entityId']]

            all_rel_retrieved_evidences.sort(key=lambda evidence: evidence.similarity_score, reverse=True)

            if logger.level == logging.DEBUG:
                logger.debug(' ')
                logger.debug(query_table.identifier)
                logger.debug(row['entityId'])
                for evidence in all_rel_retrieved_evidences:
                    logger.debug(evidence.table)
                    logger.debug(evidence.row_id)
                    logger.debug(evidence.similarity_score)

            if query_table.type == 'augmentation':
                result.target_values[row['entityId']] = row[query_table.target_attribute]

            for k in k_interval:
                if k == 1 and voting == 'weighted':
                    continue

                rel_retrieved_evidences = all_rel_retrieved_evidences[:k]
                no_rel_evidences = sum([1 for _ in rel_retrieved_evidences])

                no_verified_evidences = sum(
                    [1 for evidence in positive_evidences if evidence.entity_id == row['entityId']])
                no_pos_evidences = sum([1 for evidence in rel_retrieved_evidences if evidence in positive_evidences])

                # Calculate precision at k
                precision = 0
                if no_rel_evidences > 0:
                    precision = no_pos_evidences / no_rel_evidences
                result.precision_per_entity[k][row['entityId']] = precision

                # Calculate recall at k
                recall = 0
                if no_verified_evidences > 0:
                    recall = no_pos_evidences / min(no_verified_evidences, k)

                result.recall_per_entity[k][row['entityId']] = recall

                f1 = 0
                if (precision + recall) > 0:
                    f1 = (2 * precision * recall) / (precision + recall)
                result.f1_per_entity[k][row['entityId']] = f1

                # Calculate not annotated
                no_not_annotated = 0
                if no_rel_evidences > 0:
                    no_not_annotated = sum([1 for evidence in rel_retrieved_evidences
                                            if evidence not in positive_evidences
                                            and evidence not in negative_evidences]) / no_rel_evidences

                result.no_known_relevant_evidences[k][row['entityId']] = no_pos_evidences
                result.no_verified_evidences[k][row['entityId']] = no_verified_evidences
                result.not_annotated_per_entity[k][row['entityId']] = no_not_annotated

                no_retrieved_evidences = 0
                result_evidences = []
                for retrieved_evidence in rel_retrieved_evidences[:k]:
                    for verified_evidence in [evidence for evidence in positive_evidences if evidence.entity_id == row['entityId']]:
                        if retrieved_evidence == verified_evidence:
                            if verified_evidence.corner_case:
                                no_retrieved_evidences += 1
                            break

                    if retrieved_evidence.context is not None:
                        # Add evidence information to context
                        result_evidence = retrieved_evidence.context.copy()
                        result_evidence['similarity_score'] = retrieved_evidence.similarity_score
                        result_evidence['relevant_evidence'] = True if retrieved_evidence in positive_evidences else False
                        result_evidence['table'] = retrieved_evidence.table
                        result_evidence['row_id'] = retrieved_evidence.row_id
                        result_evidences.append(result_evidence)

                found_positive_evidences = [evidence for evidence in positive_evidences
                                              if evidence.entity_id == row['entityId']]
                if len(found_positive_evidences) > 0:
                    result.seen_training[k][row['entityId']] = found_positive_evidences[0].seen_training
                else:
                    result.seen_training[k][row['entityId']] = None

                # Count number of corner cases
                result.corner_cases[k][row['entityId']] = sum([1 for evidence in positive_evidences
                                                               if evidence.entity_id == row['entityId']
                                                               and evidence.corner_case])
                result.retrieved_corner_cases[k][row['entityId']] = no_retrieved_evidences

                result.different_evidences[k][row['entityId']] = result_evidences
                result.different_tables[k][row['entityId']] = \
                    list(set([evidence.table for evidence in rel_retrieved_evidences][:k]))

                if experiment_type == 'augmentation':
                    values = []
                    similarities = []
                    sequence_scores = []
                    dict_value_sequence_score = {}

                    for evidence in rel_retrieved_evidences[:k]:
                        # Exclude evidence value from augmentation if it is None
                        if evidence.value is not None:
                            if type(evidence.value) is str:
                                value = evidence.value
                            elif type(evidence.value) is list:
                                value = ', '.join(evidence.value)
                            else:
                                value = str(evidence.value)

                            values.append(value)
                            similarities.append(evidence.similarity_score)
                        if 'sequence_scores' in evidence.scores and evidence.scores['sequence_scores'] is not None:
                            sequence_scores.append(evidence.scores['sequence_scores'])
                        else:
                            sequence_scores.append(0)

                    # simple voting vs. weighted voting - To-Do: Separate fusion from evaluation!
                    if voting == 'simple':
                        value_counts = [(value, values.count(value)) for value in set(values)]
                    elif voting == 'weighted':
                        dict_value_similarity = {}
                        
                        total_similarity = 0
                        initial_value_counts = { value: values.count(value) for value in set(values)}
                        for value, similarity_score, sequence_score in zip(values, similarities, sequence_scores):
                            if value not in dict_value_similarity:
                                dict_value_similarity[value] = 0
                            dict_value_similarity[value] += similarity_score
                            total_similarity += similarity_score
                            dict_value_sequence_score[value] = sequence_score

                        # Normalize similarity scores by number of appearances
                        dict_value_norm_similarity = {value: sim/initial_value_counts[value]
                                                       for value, sim in dict_value_similarity.items()}
                        if total_similarity > 0:
                            value_counts = [(value, sim/total_similarity) for value, sim in dict_value_norm_similarity.items()]
                        else:
                            value_counts = [(value, 0) for value, sim in
                                            dict_value_norm_similarity.items()]

                    else:
                        raise ValueError('Unknown voting strategy {}.'.format(voting))

                    dict_value_counts = [{'value': value_count[0], 'count': value_count[1]} for value_count in value_counts]
                    for dict_value_count in dict_value_counts:
                        if dict_value_count['value'] in dict_value_sequence_score:
                            dict_value_count['sequence_score'] = dict_value_sequence_score[dict_value_count['value']]
                            continue
                    value_counts.sort(key=lambda x: x[1], reverse=True)

                    # Calculate Accuracy
                    accuracy = 0
                    predicted_value = None
                    if len(value_counts) > 0:
                        datatype = get_datatype(query_table.target_attribute)
                        if datatype == 'coordinate':
                            # TO-DO: Replace hack with proper approach to retrieve full coordinates!
                            target_value, predicted_value = determine_full_coordinates(value_counts[0][0],
                                                                                       query_table.target_attribute, row,
                                                                                       rel_retrieved_evidences[:k])
                            accuracy = calculate_accuracy(target_value, predicted_value, datatype)

                        else:
                            target_value = row[query_table.target_attribute]
                            predicted_value = value_counts[0][0]

                            accuracy = calculate_accuracy(target_value, predicted_value, datatype)

                    result.fusion_accuracy[k][row['entityId']] = accuracy
                    result.different_values[k][row['entityId']] = dict_value_counts
                    result.found_values[k][row['entityId']] = len(values)
                    result.predicted_values[k][row['entityId']] = predicted_value

        results.append(result)

    return results


def determine_full_coordinates(predicted_coordinate_part, target_attribute, row, rel_evidences):
    """Determine full coordinates"""
    complementary_attribute = {'latitude': 'longitude', 'longitude': 'latitude'}
    predicted_dict = {target_attribute : predicted_coordinate_part,
                      complementary_attribute[target_attribute] : 0}
    target_dict = {target_attribute: row[target_attribute],
                   complementary_attribute[target_attribute]: row[complementary_attribute[target_attribute]]}

    for evidence in rel_evidences:
        if normalize_value(evidence.context[target_attribute], 'coordinate', None) == predicted_coordinate_part:
            if complementary_attribute[target_attribute] in evidence.context:
                predicted_dict[complementary_attribute[target_attribute]] = \
                    evidence.context[complementary_attribute[target_attribute]]
                break

    # Normalize values
    target_value = (normalize_value(target_dict['longitude'], 'coordinate', None),
                    normalize_value(target_dict['latitude'], 'coordinate', None))
    predicted_value = (normalize_value(predicted_dict['longitude'], 'coordinate', None),
                       normalize_value(predicted_dict['latitude'], 'coordinate', None))

    return target_value, predicted_value


def calculate_accuracy(predicted_value, target_value, data_type):
    """Calculate the accuracy for the two provided values based on the data type"""
    accuracy = 0
    if data_type == 'coordinate':
        try:
            dist = haversine(target_value[0], target_value[1], predicted_value[0], predicted_value[1])
            # Accurarcy == 1 if the distance of the points is 100 m at max
            accuracy = 1 if dist <= 0.1 else 0
        except TypeError as e:
            logger = logging.getLogger()
            logger.warning(e)

    else:
        accuracy = 1 if target_value == predicted_value else 0
    return accuracy
