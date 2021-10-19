import pyodbc
from Utils import CSVtoLISTDICT, connectDB, connectPostgre

#print(pyodbc.drivers())

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def insert_db(cursor, data, query, chunk_size=100):
    for chunk in chunks(data, chunk_size):
        cursor.executemany(query, chunk)
        cursor.commit()

cnxn = connectPostgre('cri98li.duckdns.org', 'Nomopoly', 'nomopoly', 'nomopoly2021')
cnxn.autocommit = False
cursor = cnxn.cursor()
cursor.fast_executemany = True

dates = CSVtoLISTDICT('output/date.csv', True, ",")
players = CSVtoLISTDICT('output/players.csv', True, ",")
tournaments = CSVtoLISTDICT('output/tournament.csv', True, ",")
match = CSVtoLISTDICT('output/match.csv', True, ",")
countries = CSVtoLISTDICT('dati/countries.csv', True, ",")

query_date = '''INSERT INTO date (date_id, day, month, year, quarter)
                                VALUES (?, ?, ?, ?, ?)'''

query_tournament = 'INSERT INTO tournament (tourney_id, date_id, tourney_name, surface, draw_size, tourney_level, tourney_spectators, tourney_revenue)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

query_countries = 'INSERT INTO tournaments (country_ioc, continent)' \
                   '            VALUES (?, ?)'

# Insert dates
input_list = [ ( date['tourney_date'], date['day'], date['month'], date['year'], date['quarter'] ) for date in dates]
print('Insert date')

insert_db(cursor, input_list, query_date, 250)

print('Fine Insert date')

# Insert tournaments
# chiave primaria tourney_id + tourney_level + tourney_name
input_list = [ ( tourney['tourney_id'] + tourney['tourney_level'] + tourney['tourney_name'],
                 tourney['tourney_date'], tourney['tourney_name'], tourney['surface'],
                 tourney['draw_size'], tourney['tourney_level'],
                 tourney['tourney_spectators'], tourney['tourney_revenue'] ) for tourney in tournaments ]

print('Insert tournament')

insert_db(cursor, input_list, query_tournament, 250)

print('Fine Insert tournament')

cnxn.close()

