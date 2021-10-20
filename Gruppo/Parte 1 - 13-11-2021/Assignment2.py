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
matches = CSVtoLISTDICT('output/match.csv', True, ",")
countries = CSVtoLISTDICT('dati/countries.csv', True, ",")

query_date = '''INSERT INTO date (date_id, day, month, year, quarter)
                                VALUES (?, ?, ?, ?, ?)'''

query_tournament = 'INSERT INTO Tournament (tourney_id, date_id, tourney_name, surface, draw_size, tourney_level, tourney_spectators, tourney_revenue)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

query_countries = 'INSERT INTO Geography (country_ioc, country_name, continent, language)' \
                   '            VALUES (?, ?, ?, ?)'

query_players = 'INSERT INTO Player (player_id, country_id, name, sex, hand, year_of_birth)' \
                   '            VALUES (?, ?, ?, ?, ?, ?)'

query_match = 'INSERT INTO Match (match_id,winner_id,loser_id,score,best_of,round,minutes,w_ace,w_df,w_svpt,w_1stIn,' \
              '             w_1stWon,w_2ndWon,w_SvGms,w_bpSaved,w_bpFaced,l_ace,l_df,l_svpt,l_1stIn,l_1stWon,l_2ndWon,l_SvGms,' \
              '             l_bpSaved,l_bpFaced,winner_rank,winner_rank_points,loser_rank,loser_rank_points)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

# Insert tournaments
# chiave primaria tourney_id + tourney_level + tourney_name + tourney_date
input_list = [ ( tourney['tourney_id'],
                 tourney['date_id'], tourney['tourney_name'], tourney['surface'],
                 tourney['draw_size'], tourney['tourney_level'],
                 tourney['tourney_spectators'], tourney['tourney_revenue'] ) for tourney in tournaments ]
#insert(cursor, input_list, query_tournament)

input_list = [ ( country['country_code'], country['country_name'], country['continent'], country['continent'][:1] ) for country in countries ]
#insert(cursor, input_list, query_countries)

input_list = [ ( player['player_id'], player['country_id'], player['name'], player['sex'], player['hand'], player['year_of_birth'] ) for player in players ]
#insert(cursor, input_list, query_players)

input_list = [ list(match.values()) for match in matches ]

for i, tuple in enumerate(input_list):
    for j, value in enumerate(tuple):
        try:
            new_val = int(float(value))
            input_list[i][j] = new_val
        except:
            ''

print(input_list[0])
insert(cursor, input_list, query_match)

cnxn.close()

