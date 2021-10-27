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
    for i, record in enumerate(tqdm(data)):
        try:
            print(record)
            cursor.execute(query, record)
            cursor.commit()
        except pyodbc.IntegrityError as e:
            print(e)
            with open('log.txt', "w", newline='') as file:
                file.write('a' + '\n')
                file.write(str(i))
                file.close()
                return


cnxn = connectDB('131.114.72.230', 'Group_11_DB', 'Group_11', '9WGTTUCP')
cnxn.autocommit = False
cursor = cnxn.cursor()

players = CSVtoLISTDICT('output/players_noNull.csv', True, ",")
tournaments = CSVtoLISTDICT('output/tournament.csv', True, ",")
matches = CSVtoLISTDICT('output/match.csv', True, ",")
countries = CSVtoLISTDICT('output/countries.csv', True, ",")

query_tournament = 'INSERT INTO Tournament (tourney_pk, tourney_id, date_id, tourney_name, surface, draw_size, tourney_level, tourney_spectators, tourney_revenue)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'

query_countries = 'INSERT INTO Geography (country_ioc, country_name, continent, language)' \
                   '            VALUES (?, ?, ?, ?)'

query_players = 'INSERT INTO Player (player_id, country_id, name, sex, hand, year_of_birth)' \
                   '            VALUES (?, ?, ?, ?, ?, ?)'

query_match = 'INSERT INTO Match (match_id,winner_id,loser_id,score,best_of,round,minutes,w_ace,w_df,w_svpt,w_1stIn,' \
              '             w_1stWon,w_2ndWon,w_SvGms,w_bpSaved,w_bpFaced,l_ace,l_df,l_svpt,l_1stIn,l_1stWon,l_2ndWon,l_SvGms,' \
              '             l_bpSaved,l_bpFaced,winner_rank,winner_rank_points,loser_rank,loser_rank_points,tourney_id)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

# Insert tournaments
# chiave primaria tourney_id + tourney_level + tourney_date
input_list = [ ( #tourney['tourney_id'] + tourney['tourney_level'] + tourney['date_id'] + tourney['tourney_name'],
                tourney['tourney_pk'],
                 tourney['tourney_id'], tourney['date_id'],
                 tourney['tourney_name'], tourney['surface'],
                 tourney['draw_size'], tourney['tourney_level'],
                 tourney['tourney_spectators'], tourney['tourney_revenue'] ) for tourney in tournaments ]
#insert(cursor, input_list, query_tournament)

country_codes = {country['country_code'] for country in countries}
country_to_add = set()
for player in players:
    if player['country_id'] not in country_codes:
        country_to_add.add(player['country_id'])

print('Paesi da aggiungere trovati in Players: ', country_to_add)

country_list = CSVtoLISTDICT("dati/countryInfo.tsv", True, "\t")

new_countries = [ [ x['ISO3'], x['Country'], x['Continent'], x['Languages'].lower().split(",")[0] ]
                    for x in country_list if x['ISO3'] in country_to_add]

unkwown_countries = country_to_add - {c[0] for c in new_countries}

print('Paesi non recuperabili usando countryInfo.tsv', unkwown_countries)

input_list = [ list(country.values()) for country in countries ] + new_countries + [[country, 'Unknown', 'Unknown', 'Unknown'] for country in unkwown_countries ]
#insert(cursor, input_list, query_countries)


input_list = [ list(player.values()) for player in players ]
#insert(cursor, input_list, query_players)

player_ids = {player['player_id'] for player in players}
unkwown_players = set()

for match in matches:
    if match['loser_id'] not in player_ids or match['winner_id'] not in player_ids:
        unkwown_players.add(match['loser_id'])
        unkwown_players.add(match['winner_id'])

print('Giocatori sconosciuti (non presenti in players.csv)', unkwown_players)

input_list = [ [ str(match['match_num']) + match['tourney_pk'] ] + list(match.values())[1:] for match in matches ]

for i, tuple in enumerate(input_list):
    for j, value in enumerate(tuple):
        try:
            new_val = int(float(value))
            input_list[i][j] = new_val
        except:
            ''

insert(cursor, input_list, query_match)

cnxn.close()

