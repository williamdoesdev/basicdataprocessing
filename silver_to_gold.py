import pandas as pd
import os
import re

# Create dataframes
columns = ['port_of_unlading', 'month']
for i in range(1, 100):
    columns.append("hs_" + str(i).zfill(2))
hs_by_unlading = pd.DataFrame(columns=columns)

columns = ['foreign_port_of_lading', 'month']
for i in range(1, 100):
    columns.append("hs_" + str(i).zfill(2))
hs_by_unlading = pd.DataFrame(columns=columns)

columns = ['vessel_country_code', 'month']
for i in range(1, 100):
    columns.append("hs_" + str(i).zfill(2))
hs_by_unlading = pd.DataFrame(columns=columns)

hs_codes = pd.read_csv(
    'datalake/layer=silver/table=hscodes/hscodes.csv')

# Header table
for year_folder in os.listdir('datalake/layer=silver/table=header'):
    for header_file in os.listdir('datalake/layer=silver/table=header/' + year_folder):

        filepath = 'datalake/layer=silver/table=header/' + year_folder + '/' + header_file
        chunk = pd.read_csv(filepath)

        chunk['harmonized_number'] = chunk['harmonized_number'].apply(
            lambda x: int(re.sub('\D', '', str(x)[0:2])))
