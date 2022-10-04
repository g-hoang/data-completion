import logging

import click
import csv
import pandas as pd

@click.command()
@click.option('--path_to_tables', type=click.Path(exists=True))
@click.option('--path_to_output')
def sample_query_tables_from_imdb(path_to_tables, path_to_output):
    """Sample Query tables from IMDB
        Start with IMDB title basic"""
    logger = logging.getLogger()

    # IMDB data table
    path_to_imdb_table = path_to_tables + 'Movie_imdb.com_September2020.json.gz'

    df_imdb = pd.read_csv(path_to_imdb_table, sep='\t', compression='gzip')
    logger.info('Data loaded')
    movie_names = ["2 Guns", "Harry Potter and the Chamber of Secrets", "Harry Potter and the Deathly Hallows: Part 1",
                    "Harry Potter and the Deathly Hallows: Part 2", "Inception", "Ocean's Eight", "Ocean's Eleven",
                    "The Commuter", "Titanic"]

    df_imdb = df_imdb[df_imdb['Name'].isin(movie_names)]
    df_imdb = df_imdb[['Name', 'Datpublished', 'Duration', 'Director.Name', 'Actor.Name']]

    print(df_imdb)



    logger.info('Query tables written to file')

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    sample_query_tables_from_imdb()