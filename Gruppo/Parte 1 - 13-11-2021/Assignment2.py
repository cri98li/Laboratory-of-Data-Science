import pyodbc
from Utils import CSVtoLISTDICT, connectDB, connectPostgre
from tqdm import tqdm

#print(pyodbc.drivers())

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def insert_db(cursor, data, query, chunk_size=100):
    for chunk in chunks(data, chunk_size):
        try:
            cursor.executemany(query, chunk)
            cursor.commit()
        except pyodbc.IntegrityError:
            ''

def insert(cursor, data, query):
    for record in tqdm(data):
        try:
            cursor.execute(query, record)
            cursor.commit()
        except pyodbc.IntegrityError:
            ''

cnxn = connectDB('131.114.72.230', 'Group_11_DB', 'Group_11', '9WGTTUCP')
cnxn.autocommit = False
cursor = cnxn.cursor()

dates = CSVtoLISTDICT('output/date.csv', True, ",")
players = CSVtoLISTDICT('output/players.csv', True, ",")
tournaments = CSVtoLISTDICT('output/tournament_noNull.csv', True, ",")
match = CSVtoLISTDICT('output/match.csv', True, ",")
countries = CSVtoLISTDICT('dati/countries.csv', True, ",")

query_date = '''INSERT INTO date (date_id, day, month, year, quarter)
                                VALUES (?, ?, ?, ?, ?)'''

query_tournament = 'INSERT INTO Tournament (tourney_id, date_id, tourney_name, surface, draw_size, tourney_level, tourney_spectators, tourney_revenue)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

query_countries = 'INSERT INTO tournaments (country_ioc, continent)' \
                   '            VALUES (?, ?)'

# Insert tournaments
# chiave primaria tourney_id + tourney_level + tourney_name + tourney_date
input_list = [ ( tourney['tourney_id'],
                 tourney['date_id'], tourney['tourney_name'], tourney['surface'],
                 tourney['draw_size'], tourney['tourney_level'],
                 tourney['tourney_spectators'], tourney['tourney_revenue'] ) for tourney in tournaments ]

print('Insert tournament')

insert(cursor, input_list, query_tournament)

print('Fine Insert tournament')

cnxn.close()

