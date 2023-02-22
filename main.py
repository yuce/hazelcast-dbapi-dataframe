import logging

import pandas
from hazelcast.db import connect

# configure the logger
logging.basicConfig(level=logging.INFO)

# get a connection to the cluster with the DBAPI interface
conn = connect()
# create a cursor
cursor = conn.cursor()
# create a mapping
cursor.execute('''
    CREATE OR REPLACE MAPPING city (
    __key INT,
    country VARCHAR,
    name VARCHAR)
    TYPE IMap
    OPTIONS('keyFormat'='int', 'valueFormat'='json-flat');
''')
# add some data
cursor.execute('''
    SINK INTO city VALUES
    (1, 'United Kingdom','London'),
(2, 'United Kingdom','Manchester'),
(3, 'United States', 'New York'),
(4, 'United States', 'Los Angeles'),
(5, 'Turkey', 'Ankara'),
(6, 'Turkey', 'Istanbul'),
(7, 'Brazil', 'Sao Paulo'),
(8, 'Brazil', 'Rio de Janeiro');
''')
# create a dataframe from SQL
df = pandas.read_sql("select * from city", conn)
print(df)

