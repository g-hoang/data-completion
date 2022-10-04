import logging
import click
from statistics import median

from src.strategy.open_book.es_helper import determine_es_index_name
from src.strategy.open_book.retrieval.retrieval_strategy import RetrievalStrategy
from src.preprocessing.language_detection import LanguageDetector

@click.command()
@click.option('--schema_org_class')
@click.option('--field')
@click.option('--size', default=1000)

def main(schema_org_class, field, size):
    logger = logging.getLogger()
    logger.info(f'Start extract statistics from {schema_org_class} index')

    extract_statistic_strategy = RetrievalStrategy(schema_org_class, 'extract statistics')
    ld = LanguageDetector()

    last_page = False
    search_after = 0
    index_name = determine_es_index_name(schema_org_class)
    results = []
    no_nonEnglish_entity = 0
    wordCounts = []
    sentenceCounts = []

    while not last_page:
        logger.info(f'Search {schema_org_class} index from id={search_after}')
        entity_results = extract_statistic_strategy.query_by_column(field, index_name, size, search_after)

        # Counting non-English rows
        for result in entity_results:
            if ld.check_language_is_english(result['_source'][field]):
                source = result['_source'].copy()

                source['wordCount'] = len(source[field].split())
                wordCounts.append(len(source[field].split()))

                source['sentenceCount'] = len(source[field].split('.'))
                sentenceCounts.append(len(source[field].split('.')))
                results.append(source)
                logger.info(f'Number of accepted rows: {len(results)}')
            else:
                no_nonEnglish_entity += 1

        if len(entity_results) < size:
            last_page = True
        else:
            search_after = entity_results[size-1]["sort"][0]
    
    logger.info(f'Number of non-English rows: {no_nonEnglish_entity}')
    logger.info(f'There are in total {len(wordCounts)} English rows with high confidence')

    # Calculate avg and median value from results
    wordAvg = sum(wordCounts) / len(wordCounts)
    logger.info(f'The {field} column has average {wordAvg} words in each row')

    medianAvg = median(wordCounts)
    logger.info(f'The {field} column has median {medianAvg} words in each row')

    sentenceAvg = sum(sentenceCounts) / len(sentenceCounts)
    logger.info(f'The {field} column has average {sentenceAvg} sentence in each row')

    medianSentence = median(sentenceCounts)
    logger.info(f'The {field} column has median {medianSentence} sentence in each row')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    main()
