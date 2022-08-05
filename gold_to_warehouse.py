import psycopg2
import dotenv

dotenv.load_dotenv()

conn = psycopg2.connect(
    host="localhost",
    port='49153',
    database="project",
    user=dotenv.dotenv_values()['DB_USER'],
    password=dotenv.dotenv_values()['DB_PASSWORD'])

cursor = conn.cursor()


# hs_codes

# Open file
fileInput = open('datalake/layer=gold/table=hscodes/hs_codes.csv', "r")

# Build SQL code to drop table if exists and create table
sqlQueryCreate = 'DROP TABLE IF EXISTS hs_codes;\nCREATE TABLE hs_codes(hs_code VARCHAR(64), description VARCHAR(512), PRIMARY KEY (hs_code));'

cursor.execute(sqlQueryCreate)
conn.commit()


with open('datalake/layer=gold/table=hscodes/hs_codes.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'hs_codes', sep=',')

conn.commit()


# hs_by_unlading

# Open file
fileInput = open(
    'datalake/layer=gold/table=hs_by_unlading/hs_by_unlading.csv', "r")

# Build SQL code to drop table if exists and create table
sqlQueryCreate = 'DROP TABLE IF EXISTS hs_by_unlading;\nCREATE TABLE hs_by_unlading(arrival_month VARCHAR(32), port_of_unlading VARCHAR(256),'

for i in range(1, 100):
    sqlQueryCreate += ' hs_' + str(i) + ' FLOAT,'

sqlQueryCreate += 'PRIMARY KEY (arrival_month, port_of_unlading));'

cursor.execute(sqlQueryCreate)
conn.commit()


with open('datalake/layer=gold/table=hs_by_unlading/hs_by_unlading.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'hs_by_unlading', sep=',')

conn.commit()


# hs_by_lading

# Open file
fileInput = open(
    'datalake/layer=gold/table=hs_by_lading/hs_by_lading.csv', "r")

# Build SQL code to drop table if exists and create table
sqlQueryCreate = 'DROP TABLE IF EXISTS hs_by_lading;\nCREATE TABLE hs_by_lading(arrival_month VARCHAR(32), foreign_port_of_lading VARCHAR(256),'

for i in range(1, 100):
    sqlQueryCreate += ' hs_' + str(i) + ' FLOAT,'

sqlQueryCreate += 'PRIMARY KEY (arrival_month, foreign_port_of_lading));'

cursor.execute(sqlQueryCreate)
conn.commit()


with open('datalake/layer=gold/table=hs_by_lading/hs_by_lading.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'hs_by_lading', sep=',')

conn.commit()


# hs_by_vessel_country_code

# Open file
fileInput = open(
    'datalake/layer=gold/table=hs_by_vessel_country_code/hs_by_vessel_country_code.csv', "r")

# Build SQL code to drop table if exists and create table
sqlQueryCreate = 'DROP TABLE IF EXISTS hs_by_vessel_country_code;\nCREATE TABLE hs_by_vessel_country_code(arrival_month VARCHAR(32), vessel_country_code VARCHAR(8),'

for i in range(1, 100):
    sqlQueryCreate += ' hs_' + str(i) + ' FLOAT,'

sqlQueryCreate += 'PRIMARY KEY (arrival_month, vessel_country_code));'

cursor.execute(sqlQueryCreate)
conn.commit()


with open('datalake/layer=gold/table=hs_by_vessel_country_code/hs_by_vessel_country_code.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'hs_by_vessel_country_code', sep=',')

conn.commit()
