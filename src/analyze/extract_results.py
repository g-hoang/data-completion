import json
import itertools
import logging
import click
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np

@click.command()
@click.option('--path_to_file', required=True, help='Path to the result file')
@click.option('--strategy', required=True, help='Strategy of the model producing this result file. Can be `augmentation` or `retrieval`')
@click.option('--alphas', required=False, help='Alpha value used as confidence level of the model, can be array', default="0")

def getOverallResult(path_to_file, strategy, alphas):
    logger = logging.getLogger()

    result_file = []
    if ',' in alphas:
        alphas = alphas.split(',')

    try:
        with open(path_to_file, 'r') as json_file:
            logger.info("Opening file...")
            json_list = list(json_file)
            for json_str in json_list:
                result = json.loads(json_str)
                result_file.append(result)
    except:
        logger.warning('Fail to open file')
        return

    # Compare predicted value and correct value
    if strategy == 'augmentation':
        correctPredictionCounter = 0
        for prediction in result_file:
            for different_value in prediction['different_values']:
                if prediction['target_value'] == different_value['value']:
                    correctPredictionCounter += 1
                    break
            if prediction['target_value'] == prediction['predicted_value']:
                prediction['isCorrect'] = True
            else:
                prediction['isCorrect'] = False

                # Handle cases where the target value has more than one entity
                if isinstance(prediction['target_value'], list) and ',' in prediction['predicted_value']:
                    prediction['predicted_value'] = prediction['predicted_value'].split(',')
                    prediction['predicted_value'] = [item.strip() for item in prediction['predicted_value']]
                    if set(prediction['target_value']) == set(prediction['predicted_value']):
                        prediction['isCorrect'] = True

                # Calculate partial matches

                # Detect partial match of datepublished and duration
                if prediction['targetAttribute'] == 'datepublished' or prediction['targetAttribute'] == 'duration':
                    if prediction['target_value'][:4] == prediction['predicted_value'][:4]:
                        prediction['PM'] = True
                
                # Detect partial match of telephone
                if prediction['targetAttribute'] == 'telephone':
                    if prediction['target_value'][:7] == prediction['predicted_value'][:7]:
                        prediction['PM'] = True

                # Detect partial match of streetaddress
                if prediction['targetAttribute'] == 'streetaddress':
                    target_street_address_tokens = prediction['target_value'].split(' ')
                    predicted_street_address_tokens = prediction['predicted_value'].split(' ')
                    counter = 0
                    for token in predicted_street_address_tokens:
                        if token in target_street_address_tokens:
                            counter += 1
                    if (counter / len(predicted_street_address_tokens)) >= 0.5:
                        prediction['PM'] = True

                # Detect partial match of postalcode
                if prediction['targetAttribute'] == 'postalcode':
                    predicted_value = prediction['predicted_value'].replace(' ', '')
                    target_value = prediction['target_value'].replace(' ', '')
                    counter = 0
                    for idx in range(len(predicted_value)):
                        if len(target_value) > idx and predicted_value[idx] == target_value[idx]:
                            counter += 1
                    if (counter / len(predicted_value)) >= 0.5:
                        prediction['PM'] = True

            prediction['sequence_confidence'] = prediction['different_values'][0]['sequence_score']
        
        # Exporting chart
        exporting_chart(result_file)

        for alpha in alphas:
            confident_answers = [item for item in result_file if item['sequence_confidence'] >= float(alpha)]
            correct_result = [item for item in confident_answers if item.get('isCorrect') == True]

            logger.info(f"Coverage given {alpha}: {len(confident_answers)}/{len(result_file)} = {len(confident_answers)/len(result_file)}")
            logger.info(f"Precision: {len(correct_result)}/{len(confident_answers)} = {len(correct_result)/len(confident_answers)}")
            logger.info(f'P@5: {correctPredictionCounter}/{len(result_file)} = {correctPredictionCounter/len(result_file)}\n\n')

        print_incorrect_prediction(result_file)
        logger.info('Groupped by target attributes: {}'.format(grouped_accuracy(result_file, 'targetAttribute')))
        df = pd.DataFrame(result_file)
        model_name = result_file[0]['model_name'].split('/')
        plt.savefig(f"{result_file[0]['schemaOrgClass']}_{model_name[-2]}_{model_name[-1]}.png")
        df.to_excel(f"../excels/augmentation_{result_file[0]['schemaOrgClass']}_{model_name[-2]}_{model_name[-1]}.xlsx")
    elif strategy == 'retrieval':
        # Get all unique values for voting_strategy and k_intervals
        voting_strategy = ['simple', 'weighted']
        k_intervals = list(set([item['k'] for item in result_file]))

        # Define best result id
        best_result = {
            'id': 0,
            'correct_results': -1
        }
        id = 0

        results = []
        for pipeline in itertools.product(voting_strategy, k_intervals):
            # Get all results for this pipeline
            results_for_pipeline = [item for item in result_file if item['voting_strategy'] == pipeline[0] and item['k'] == pipeline[1]]
            correct_result = [item for item in result_file if item['voting_strategy'] == pipeline[0] and item['k'] == pipeline[1] \
                and item.get('target_value') == item.get('predicted_value')]
            incorrect_result = [item for item in result_file if item['voting_strategy'] == pipeline[0] and item['k'] == pipeline[1] \
                and item.get('target_value') != item.get('predicted_value')]
            results.append({
                'voting_strategy': pipeline[0],
                'k_intervals': pipeline[1],
                'total_results': len(results_for_pipeline),
                'correct_results': len(correct_result),
                'incorrect_results': len(incorrect_result)
            })
            if len(correct_result) > best_result['correct_results']:
                best_result = {
                    'id': id,
                    'correct_results': len(correct_result)
                }
            id += 1
        
        JSON_best_result = [item for item in result_file if item['voting_strategy'] == results[best_result['id']]['voting_strategy'] and \
            item['k'] == results[best_result['id']]['k_intervals']]
        incorrectPredictions = [item for item in result_file if item['voting_strategy'] == results[best_result['id']]['voting_strategy'] and item['k'] == results[best_result['id']]['k_intervals'] \
                and item.get('target_value') != item.get('predicted_value')]
        logger.info('Report for retrieval strategy: {}'.format(results))
        logger.info(f'Number of queries: {len(JSON_best_result)}')
        schemaOrgClass = JSON_best_result[0]['schemaOrgClass']
    
        with open(f'result/best_results/{strategy}_{schemaOrgClass}.json', 'a+') as f:
            json.dump(JSON_best_result, f)

        with open(f'result/best_results/error_from_the_best_{strategy}_{schemaOrgClass}.json', 'a+') as f:
            json.dump(incorrectPredictions, f)
        
        logger.info('Groupped by target attributes: {}'.format(grouped_accuracy(JSON_best_result, 'targetAttribute')))
        df = pd.DataFrame(result_file)
        model_name = result_file[0]['model_name']
        df.to_excel(f"../excels/augmentation_{result_file[0]['schemaOrgClass']}_{model_name}.xlsx")
    return result_file

def exporting_chart(result_file):
    total = np.array([item['sequence_confidence'] for item in result_file])
    true_confidence = np.array([item['sequence_confidence'] for item in result_file if item['isCorrect'] == True])
    false_confidence = np.array([item['sequence_confidence'] for item in result_file if item['isCorrect'] == False])

    data = [total, true_confidence, false_confidence]

    fig = plt.figure(figsize =(10, 7))
    # fig, ax = plt.subplots()
    plt.boxplot(data, labels=['Total', 'Correct', 'Incorrect'])
    plt.ylabel('Confidence score')
    quantiles_total = np.quantile(total, [0.25, 0.5])
    quantiles_correct = np.quantile(true_confidence, np.array([0.00]))
    quantiles_incorrect = np.quantile(false_confidence, np.array([0.00, 0.25, 0.75]))
    plt.hlines(quantiles_total, [0] * quantiles_total.size, [1] * quantiles_total.size, color='blue', ls=':', lw=0.5, zorder=0)
    plt.hlines(quantiles_correct, [0] * quantiles_correct.size, [2] * quantiles_correct.size, color='green', ls=':', lw=0.5, zorder=0)
    plt.hlines(quantiles_incorrect, [0] * quantiles_incorrect.size, [3] * quantiles_incorrect.size, color='red', ls=':', lw=0.5)
    quantiles = np.hstack([quantiles_total, quantiles_correct,quantiles_incorrect])
    plt.yticks(quantiles)
    plt.title(f"Sequence scores of queries in {result_file[0]['schemaOrgClass']} categories")

def grouped_accuracy(selected_results, groupped_attribute):
    # Get the correct result for each attribute
    correct_result_attr = {}
    for item in selected_results:
        if item[groupped_attribute] not in correct_result_attr.keys():
            correct_result_attr[item[groupped_attribute]] = {'correct': 0, 'PM': 0, 'total': 0}
        if item['isCorrect'] == True:
            correct_result_attr[item[groupped_attribute]]['correct'] += 1
        elif 'PM' in item and item['PM'] == True:
            correct_result_attr[item[groupped_attribute]]['PM'] += 1
            
        correct_result_attr[item[groupped_attribute]]['total'] += 1
    return correct_result_attr

def print_incorrect_prediction(selected_results):
    results = []
    for item in selected_results:
        if item['target_value'] != item['predicted_value']:
            results.append(item)
    with open(f'result/incorrect_prediction_added_years.json', 'a+') as f:
        json.dump(results, f)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    getOverallResult()
