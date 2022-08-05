import pandas as pd
import os
import re
import numpy as np
import shutil

hs_codes = pd.read_csv(
    'datalake/layer=silver/table=hscodes/hscodes.csv')

# hs_by_unlading

hs_by_unlading = pd.DataFrame()

for year_folder in os.listdir('datalake/layer=silver/table=header'):
    for header_file in os.listdir('datalake/layer=silver/table=header/' + year_folder):

        print(header_file)

        filepath = 'datalake/layer=silver/table=header/' + year_folder + '/' + header_file
        chunk = pd.read_csv(filepath)

        chunk['harmonized_number'] = chunk['harmonized_number'].apply(
            lambda x: int(re.sub('\D', '', str(x)[0:2])))

        chunk['actual_arrival_date'] = chunk['actual_arrival_date'].apply(
            lambda x: re.search('\d*-\d*', x).group())
        chunk.rename(
            columns={'actual_arrival_date': 'arrival_month'}, inplace=True)

        # Group by month and port
        unlading = chunk.groupby(['arrival_month', 'port_of_unlading'])[
            'harmonized_number'].apply(list).reset_index()

        # Create 2d array to temporarily store data
        two_d_array = np.zeros((len(unlading), 100))

        # Iterate through harmonized_numbers and add to 2d array
        for (column_name, column_data) in unlading.iterrows():
            for i, element in enumerate(column_data['harmonized_number']):
                two_d_array[column_name][element] = two_d_array[column_name][element] + 1

        new = pd.DataFrame(two_d_array)

        unlading = pd.concat([unlading, new], axis=1)

        unlading.drop(['harmonized_number'], axis=1, inplace=True)

        hs_by_unlading = pd.concat([hs_by_unlading, unlading], axis=0)
        hs_by_unlading = hs_by_unlading.groupby(
            ['arrival_month', 'port_of_unlading']).sum().reset_index()

# drop the 0 column
hs_by_unlading.drop(hs_by_unlading.columns[2], axis=1, inplace=True)

if not os.path.exists('datalake/layer=gold/table=hs_by_unlading/'):
    os.makedirs('datalake/layer=gold/table=hs_by_unlading/')
hs_by_unlading.to_csv(
    'datalake/layer=gold/table=hs_by_unlading/hs_by_unlading.csv', index=False)


# hs_by_lading

hs_by_lading = pd.DataFrame()

for year_folder in os.listdir('datalake/layer=silver/table=header'):
    for header_file in os.listdir('datalake/layer=silver/table=header/' + year_folder):

        print(header_file)

        filepath = 'datalake/layer=silver/table=header/' + year_folder + '/' + header_file
        chunk = pd.read_csv(filepath)

        chunk['harmonized_number'] = chunk['harmonized_number'].apply(
            lambda x: int(re.sub('\D', '', str(x)[0:2])))

        chunk['actual_arrival_date'] = chunk['actual_arrival_date'].apply(
            lambda x: re.search('\d*-\d*', x).group())
        chunk.rename(
            columns={'actual_arrival_date': 'arrival_month'}, inplace=True)

        # Group by month and port
        lading = chunk.groupby(['arrival_month', 'foreign_port_of_lading'])[
            'harmonized_number'].apply(list).reset_index()

        # Create 2d array to temporarily store data
        two_d_array = np.zeros((len(lading), 100))

        # Iterate through harmonized_numbers and add to 2d array
        for (column_name, column_data) in lading.iterrows():
            for i, element in enumerate(column_data['harmonized_number']):
                two_d_array[column_name][element] = two_d_array[column_name][element] + 1

        new = pd.DataFrame(two_d_array)

        lading = pd.concat([lading, new], axis=1)

        lading.drop(['harmonized_number'], axis=1, inplace=True)

        hs_by_lading = pd.concat([hs_by_lading, lading], axis=0)
        hs_by_lading = hs_by_lading.groupby(
            ['arrival_month', 'foreign_port_of_lading']).sum().reset_index()

# drop the 0 column
hs_by_lading.drop(hs_by_lading.columns[2], axis=1, inplace=True)

if not os.path.exists('datalake/layer=gold/table=hs_by_lading/'):
    os.makedirs('datalake/layer=gold/table=hs_by_lading/')
hs_by_lading.to_csv(
    'datalake/layer=gold/table=hs_by_lading/hs_by_lading.csv', index=False)


# hs_by_vessel_country_code

hs_by_vessel_country_code = pd.DataFrame()

for year_folder in os.listdir('datalake/layer=silver/table=header'):
    for header_file in os.listdir('datalake/layer=silver/table=header/' + year_folder):

        print(header_file)

        filepath = 'datalake/layer=silver/table=header/' + year_folder + '/' + header_file
        chunk = pd.read_csv(filepath)

        chunk['harmonized_number'] = chunk['harmonized_number'].apply(
            lambda x: int(re.sub('\D', '', str(x)[0:2])))

        chunk['actual_arrival_date'] = chunk['actual_arrival_date'].apply(
            lambda x: re.search('\d*-\d*', x).group())
        chunk.rename(
            columns={'actual_arrival_date': 'arrival_month'}, inplace=True)

        # Group by month and port
        vessel_country_code = chunk.groupby(['arrival_month', 'vessel_country_code'])[
            'harmonized_number'].apply(list).reset_index()

        # Create 2d array to temporarily store data
        two_d_array = np.zeros((len(vessel_country_code), 100))

        # Iterate through harmonized_numbers and add to 2d array
        for (column_name, column_data) in vessel_country_code.iterrows():
            for i, element in enumerate(column_data['harmonized_number']):
                two_d_array[column_name][element] = two_d_array[column_name][element] + 1

        new = pd.DataFrame(two_d_array)

        vessel_country_code = pd.concat([vessel_country_code, new], axis=1)

        vessel_country_code.drop(['harmonized_number'], axis=1, inplace=True)

        hs_by_vessel_country_code = pd.concat(
            [hs_by_vessel_country_code, vessel_country_code], axis=0)
        hs_by_vessel_country_code = hs_by_vessel_country_code.groupby(
            ['arrival_month', 'vessel_country_code']).sum().reset_index()

# drop the 0 column
hs_by_vessel_country_code.drop(
    hs_by_vessel_country_code.columns[2], axis=1, inplace=True)

if not os.path.exists('datalake/layer=gold/table=hs_by_vessel_country_code/'):
    os.makedirs('datalake/layer=gold/table=hs_by_vessel_country_code/')
hs_by_vessel_country_code.to_csv(
    'datalake/layer=gold/table=hs_by_vessel_country_code/hs_by_vessel_country_code.csv', index=False)


if not os.path.exists('datalake/layer=gold/table=hscodes/'):
    os.makedirs('datalake/layer=gold/table=hscodes/')
shutil.copy('datalake/layer=silver/table=hscodes/hscodes.csv',
            'datalake/layer=gold/table=hscodes/hs_codes.csv')
