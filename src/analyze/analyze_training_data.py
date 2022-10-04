import logging
import os
import click
import spacy
from tqdm import tqdm
import re

spacy.prefer_gpu()
nlp = spacy.load("en_core_web_sm")
import json

@click.command()
@click.option('--schema_org_class', required=True, help='The category dataset to analyze')
@click.option('--task', help='Analyze task', default='analyze_training_data')

def main(schema_org_class, task):
    if task == 'analyze_training_data':
        analyze_training_data(schema_org_class)
    else:
        comparing_years()

def analyze_training_data(schema_org_class):
    logger = logging.getLogger()
    logger.info('Start analyzing...')

    if schema_org_class == None:
        logger.warning('Path to result file is required')
        return

    path_to_file = f"{os.environ['DATA_DIR']}/fine-tuning/closed_book/origin/{schema_org_class}_finetuning_train.json"

    try:
        with open(path_to_file, 'r') as json_file:
            logger.info("Opening file...")
            dataset = [json.loads(line) for line in json_file]
    except:
        logger.warning('Fail to open file')
        return

    sources = [data['table_augmentation']['source'] for data in dataset]
    targets = [data['table_augmentation']['target'] for data in dataset]
    titles = []

    logger.info(f"Collecting name of entities")
    for source in tqdm(sources):
        if '[COL]name[VAL]' in source:
            title = source.replace('[COL]name[VAL]', '')
            if '[COL]' in title:
                title = title.split('[COL]')[0]
        else:
            title = None

        titles.append(title)

    logger.info(f"Start processing by spacy")
    docs = nlp.pipe(titles, disable=['textcat'])

    logger.info("Collecting entities and their labels")
    ents = []
    for doc in tqdm(docs):
        ents.append([{ent.text: ent.label_} for ent in doc.ents ])
    counts = {}

    with open(f"analyze/analyze_{schema_org_class}_finetune_data.jsonl", 'w') as outfile:
        logger.info("Writing analyze file...")
        results = []
        for idx in tqdm(range(len(ents))):
            entry = ents[idx]
            if len(entry) > 0:
                for obj in entry:
                    for key, value in obj.items():
                        if value not in counts and key != 'source':
                            counts[value] = 1
                        else:
                            counts[value] += 1
                        if value == 'DATE' and '. target:[COL]datepublished' in sources[idx]:
                            results.append({'target': targets[idx], 'date': key})
                json.dump(entry, outfile)
                outfile.write('\n')

    with open(f"analyze/extract_year_from_movie.jsonl", 'w') as years:
        logger.info("Writing comparing years file...")
        comparing_years_counter = {
            'matched_year': 0,
            'incorrect_year': 0,
            'no_year': 0
        }
        for result in tqdm(results):
            target = result['target'].replace('[VAL]', '')
            query = "([1-2][0,1,2,8,9][0-9]{2})"
            match = re.search(query, result['date'])

            if match:
                result['dateContainsYear'] = True
                start = match.start()
                year = result['date'][start:start+4]
                if year == target[:4]:
                    result['match'] = True
                    comparing_years_counter['matched_year'] += 1
                else:
                    result['match'] = False
                    comparing_years_counter['incorrect_year'] += 1
            else:
                result['dateContainsYear'] = False
                result['match'] = False
                comparing_years_counter['no_year'] += 1
            json.dump(result, years)
            years.write('\n')

    logger.info(counts)
    logger.info(f"Number of target = datepublished: {len([source for source in sources if '. target:[COL]datepublished' in source])}")
    logger.info(f"Comparing years counter: {comparing_years_counter}")

def comparing_years():
    logger = logging.getLogger()
    logger.info('Start comparing years...')

    path_to_file = f"./analyze/extract_year_from_movie.jsonl"

    try:
        with open(path_to_file, 'r') as json_file:
            logger.info("Opening file...")
            dataset = [json.loads(line) for line in json_file]
    except:
        logger.warning('Fail to open file')
        return

    count = {'correct_year': 0, 'wrong_year': 0}
    for data in dataset:
        source = data['source'].split('[COL]datepublished[VAL]')
        if len(source) > 1:
            datepublished = source[1]
        else:
            datepublished = source[0]
        if datepublished[:4] == data['date']:
            count['correct_year'] += 1
        else:
            count['wrong_year'] += 1

    logger.info(count)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    main()
