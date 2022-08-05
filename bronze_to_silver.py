import pandas as pd
import os
import re


# Header table
for year_folder in os.listdir('datalake/layer=bronze/table=header'):

    # Try to open file
    try:
        file_path = 'datalake/layer=bronze/table=header/' + year_folder + '/' + \
            os.listdir('datalake/layer=bronze/table=header/' + year_folder)[0]
    except:
        print('No file in ' + year_folder)
        continue

    # Make directory for silver
    if not os.path.exists('datalake/layer=silver/table=header/' + year_folder):
        os.makedirs('datalake/layer=silver/table=header/' + year_folder)

    # Read, process, and chunkify file
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=100000)):

        # Drop rows with na
        chunk.dropna(
            subset=['identifier', 'vessel_country_code', 'port_of_unlading', 'foreign_port_of_lading', 'actual_arrival_date'], inplace=True)

        # Drop columns we don't want
        chunk.drop(columns=chunk.columns.difference(
            ['identifier', 'vessel_country_code', 'port_of_unlading', 'foreign_port_of_lading', 'actual_arrival_date']), inplace=True)

        # Write chunk to file
        chunk.to_csv(
            f'datalake/layer=silver/table=header/{year_folder}/header_part{i}.csv', index=False)


# Tariff table
for year_folder in os.listdir('datalake/layer=bronze/table=tariff'):

    # Try to open file
    try:
        file_path = 'datalake/layer=bronze/table=tariff/' + year_folder + '/' + \
            os.listdir('datalake/layer=bronze/table=tariff/' + year_folder)[0]
    except:
        print('No file in ' + year_folder)
        continue

    # Make directory for silver
    if not os.path.exists('datalake/layer=silver/table=tariff/' + year_folder):
        os.makedirs('datalake/layer=silver/table=tariff/' + year_folder)

    # Read, process, and chunkify file
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=100000)):

        # Drop rows with na
        chunk.dropna(
            subset=['identifier', 'harmonized_number'], inplace=True)

        # Drop columns we don't want
        chunk.drop(columns=chunk.columns.difference(
            ['identifier', 'harmonized_number']), inplace=True)

        # Write chunk to file
        chunk.to_csv(
            f'datalake/layer=silver/table=tariff/{year_folder}/tariff_part{i}.csv', index=False)


# Container table
for year_folder in os.listdir('datalake/layer=bronze/table=container'):

    # Try to open file
    try:
        file_path = 'datalake/layer=bronze/table=container/' + year_folder + '/' + \
            os.listdir(
                'datalake/layer=bronze/table=container/' + year_folder)[0]
    except:
        print('No file in ' + year_folder)
        continue

    # Make directory for silver
    if not os.path.exists('datalake/layer=silver/table=container/' + year_folder):
        os.makedirs('datalake/layer=silver/table=container/' + year_folder)

    # Read, process, and chunkify file
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=100000)):

        # Drop rows with na
        chunk.dropna(
            subset=['identifier', 'load_status'], inplace=True)

        # Drop columns we don't want
        chunk.drop(columns=chunk.columns.difference(
            ['identifier', 'load_status']), inplace=True)

        # Write chunk to file
        chunk.to_csv(
            f'datalake/layer=silver/table=container/{year_folder}/container_part{i}.csv', index=False)


# HS Codes table
f = open('datalake/layer=bronze/table=hscodes/htsdata.csv', 'r', encoding='utf-8')
df = pd.read_csv(f, encoding='utf-8')
df.drop(columns=df.columns.difference(
    ['HTS Number', 'Description']), inplace=True)
df.rename(columns={'HTS Number': 'hs_code',
          'Description': 'description'}, inplace=True)

df['hs_code'] = df['hs_code'].map(lambda x: int((str(x)[:2]).lstrip('0')))
# Drop rows
df.dropna(subset=['hs_code'], inplace=True)

# Save file
if not os.path.exists('datalake/layer=silver/table=hscodes/'):
    os.makedirs('datalake/layer=silver/table=hscodes/')
df.to_csv('datalake/layer=silver/table=hscodes/hscodes.csv', index=False)

# ----------------------------------------------------------------------------------------------------------------------

# Merge tariff and header tables
for year_folder in os.listdir('datalake/layer=silver/table=header'):
    print(f"year_folder is {year_folder}")
    for header_file in os.listdir('datalake/layer=silver/table=header/' + year_folder):
        print(f"header_file is {header_file}")
        filepath = 'datalake/layer=silver/table=header/' + year_folder + '/' + header_file
        chunk = pd.read_csv(filepath)
        chunk.insert(5, 'harmonized_number', [None] * len(chunk))

        for tariff_year_folder in os.listdir('datalake/layer=silver/table=tariff'):
            print(f"tariff_year_folder is {tariff_year_folder}")
            for tariff_file in os.listdir('datalake/layer=silver/table=tariff/' + tariff_year_folder):
                print(f"tariff_file is {tariff_file}")
                tariff_filepath = 'datalake/layer=silver/table=tariff/' + \
                    tariff_year_folder + '/' + tariff_file

                # Read tariff file
                tariff_chunk = pd.read_csv(tariff_filepath)

                # Format HS Code
                tariff_chunk['harmonized_number'] = tariff_chunk['harmonized_number'].apply(
                    lambda x: int(re.sub('\D', '', re.sub('\..*$', '', str(x))).zfill(2)))

                # create a merged chunk of the two tables which can be used to update the header chunk
                update_chunk = chunk.drop(columns='harmonized_number').merge(
                    tariff_chunk, on='identifier', how='left', suffixes=('_x', ''))

                # update the header chunk with the harmonized_numbers from the merged chunk
                chunk.update(update_chunk),

        # Merge container and header tables
        chunk.insert(5, 'load_status', [None] * len(chunk))

        for container_year_folder in os.listdir('datalake/layer=silver/table=container'):
            print(f"container_year_folder is {container_year_folder}")
            for container_file in os.listdir('datalake/layer=silver/table=container/' + container_year_folder):
                print(f"container_file is {container_file}")
                container_filepath = 'datalake/layer=silver/table=container/' + \
                    container_year_folder + '/' + container_file

                # Read container file
                container_chunk = pd.read_csv(container_filepath)

                # create a merged chunk of the two tables which can be used to update the header chunk
                update_chunk = chunk.drop(columns='load_status').merge(
                    container_chunk, on='identifier', how='left', suffixes=('_x', ''))

                # update the header chunk with the load_statuses from the merged chunk
                chunk.update(update_chunk),

        chunk.dropna(inplace=True)
        chunk = chunk[chunk.load_status != 'Empty']

        # Write chunk to file
        chunk.to_csv(
            filepath, index=False)
