"""Script to convert data extracted from Tableau to QueryTable format"""
import pandas as pd

def convert_duration(value, target_format):
    if target_format == 'h':
        value = value.replace('PT', '')
        value = value.replace('H', 'h ')
        value = value.replace('M', 'min')
        return value
    elif target_format == 'm':
        value = value.replace('PT', '')

        hours_in_min = 0
        min = 0
        if 'H' in value:
            values = value.split('H')
            hours_in_min = int(values[0]) * 60

            if 'M' in values[1]:
                values = values[1].split('M')
                min = int(values[0])

        min += hours_in_min
        return '{}min'.format(min)


df = pd.read_csv('C:/Users/alebrink/Documents/02_Research/TableAugmentation/table-augmentation-framework/data/corpus/oscar_movies.csv',sep='\t', encoding='utf-8')
df.columns = ['datePublished', 'runtime', 'name', 'director']
df['entityId'] = df.index

del df['director']

#df['duration'] = df['duration'].apply(convert_duration, args=['m'])

for i in df.index:
    print(df.loc[i].to_json() + ',')


