import logging

import click
import csv
import pandas as pd

@click.command()
@click.option('--path_to_data_dir', type=click.Path(exists=True))
@click.option('--path_to_imdb_title_basic')
def sample_query_tables_from_imdb(path_to_data_dir, path_to_imdb_title_basic):
    """Sample Query tables from IMDB
        Start with IMDB title basic"""
    logger = logging.getLogger()

    path_to_imdb_title_basic = '{}\\{}'.format(path_to_data_dir, path_to_imdb_title_basic)
    df_title_basic = pd.read_csv(path_to_imdb_title_basic, sep='\t', compression='gzip')
    logger.info('Data loaded')

    df_title_basic = df_title_basic[df_title_basic['titleType'].str.contains('movie')]
    df_title_basic['startYear'] = pd.to_numeric(df_title_basic['startYear'], errors='coerce', downcast='signed')
    df_title_basic['isAdult'] = pd.to_numeric(df_title_basic['isAdult'], errors='coerce', downcast='signed')
    df_title_basic['runtimeMinutes'] = pd.to_numeric(df_title_basic['runtimeMinutes'], errors='coerce', downcast='signed')
    df_title_basic = df_title_basic[(df_title_basic['startYear'] == 2010)
                                        & (df_title_basic['isAdult'] == 0)
                                        & (~df_title_basic['runtimeMinutes'].isna())
                                        & (~df_title_basic['genres'].str.contains('\\\\N'))]

    df_title_basic['startYear'] = df_title_basic['startYear'].astype(int)
    df_title_basic['runtimeMinutes'] = df_title_basic['runtimeMinutes'].astype(int)
    logger.info('Selected {} entities'.format(len(df_title_basic)))

    for i in range(0,10):
        df_title_basic = df_title_basic.sample(frac=1, random_state=42)

        df_filled_query_table = df_title_basic.copy()[:20]
        df_filled_query_table.drop(['titleType', 'tconst', 'endYear', 'originalTitle', 'isAdult'], axis=1, inplace=True)

        df_filled_query_table.columns = ['name', 'datecreated', 'duration', 'genre']
        df_filled_query_table['duration'] = df_filled_query_table['duration'].apply(convert_runtime_min_to_duration)

        #Write goldstandard Query Table to file
        gs = 'goldstandard'
        file_path_filled_query_tables = '{}\\querytables\\{}\\querytable_{}_movie_2010_{}.csv'.format(
            path_to_data_dir,gs, gs, i)
        df_filled_query_table.to_csv(file_path_filled_query_tables, sep=',', quoting=csv.QUOTE_MINIMAL, index=False)

        #Mask values and write query table to file
        df_empty_query_table = df_filled_query_table.copy()
        df_empty_query_table['duration'] = '?'
        df_empty_query_table['genre'] = '?'
        empty = 'empty'
        file_path_empty_query_tables = '{}\\querytables\\{}\\querytable_{}_movie_2010_{}.csv'.format(
            path_to_data_dir, empty, empty, i)
        df_empty_query_table.to_csv(file_path_empty_query_tables, sep=',', quoting=csv.QUOTE_MINIMAL, index=False)

    logger.info('Query tables written to file')

def convert_runtime_min_to_duration(runtime):
    hours = int(runtime / 60)
    minutes = runtime % 60
    if hours > 0:
        return 'PT{}H{}M'.format(hours, minutes)
    else:
        return 'PT{}M'.format(hours, minutes)

if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    sample_query_tables_from_imdb()