import pyodbc
from Utils import CSVtoLISTDICT, connectDB, connectPostgre
from tqdm import tqdm

#print(pyodbc.drivers())

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

#Insert di più record alla volta usando execute many
def insert_db1(cursor, data, query, chunk_size=100):
    for chunk in chunks(data, chunk_size):
        try:
            cursor.executemany(query, chunk)
            cursor.commit()
        except pyodbc.IntegrityError as e:
            print(e)
            return

#Insert di un record per volta
def insert_db2(cursor, data, query):
    for i, record in enumerate(tqdm(data)):
        try:
            cursor.execute(query, record)
            cursor.commit()
        except pyodbc.IntegrityError as e:
            print(e)
            return


cnxn = connectDB('131.114.72.230', 'Group_11_DB', 'Group_11', '9WGTTUCP')
cnxn.autocommit = False
cursor = cnxn.cursor()

players = CSVtoLISTDICT('output/players_noNull.csv', True, ",")
tournaments = CSVtoLISTDICT('output/tournament_noNull.csv', True, ",")
matches = CSVtoLISTDICT('output/match_noNull.csv', True, ",")
countries = CSVtoLISTDICT('output/countries.csv', True, ",")

query_tournament = 'INSERT INTO Tournament (tourney_pk, tourney_id, date_id, tourney_name, surface, draw_size, tourney_level, tourney_spectators, tourney_revenue)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'

query_countries = 'INSERT INTO Geography (country_ioc, country_name, continent, language)' \
                   '            VALUES (?, ?, ?, ?)'

query_players = 'INSERT INTO Player (player_id, country_id, name, sex, hand, ht, year_of_birth)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?)'

query_match = 'INSERT INTO Match (match_id,winner_id,loser_id,score,best_of,round,minutes,w_ace,w_df,w_svpt,w_1stIn,' \
              '             w_1stWon,w_2ndWon,w_SvGms,w_bpSaved,w_bpFaced,l_ace,l_df,l_svpt,l_1stIn,l_1stWon,l_2ndWon,l_SvGms,' \
              '             l_bpSaved,l_bpFaced,winner_rank,winner_rank_points,loser_rank,loser_rank_points,tourney_id)' \
                   '            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

# Insert tournaments
input_list = [  list(tourney.values())  for tourney in tournaments ]
insert_db1(cursor, input_list, query_tournament)

#Insert countries
country_codes = {country['country_code'] for country in countries}
country_to_add = set()
for player in players:
    if player['country_id'] not in country_codes:
        country_to_add.add(player['country_id'])

print('Paesi da aggiungere trovati in Players: ', country_to_add)

#Recupero dei paesi mancanti usando countryInfo.tsv
country_list = CSVtoLISTDICT("dati/countryInfo.tsv", True, "\t")
new_countries = [ [ x['ISO3'], x['Country'], x['Continent'], x['Languages'].lower().split(",")[0] ]
                    for x in country_list if x['ISO3'] in country_to_add]

unkwown_countries = country_to_add - {c[0] for c in new_countries}

print('Paesi non recuperabili usando countryInfo.tsv', unkwown_countries)

input_list = [ list(country.values()) for country in countries ] + new_countries + [[country, 'Unknown', 'Unknown', 'Unknown'] for country in unkwown_countries ]
insert_db1(cursor, input_list, query_countries)

#Insert Players
input_list = [ list(player.values()) for player in players ]
insert_db1(cursor, input_list, query_players)

input_list = [ [ str(match['match_num']) + match['tourney_pk'] + str(match['winner_id']) + str(match['loser_id']) ] + list(match.values())[1:] for match in matches ]

#Forse non necessario (cast a interi)
for i, tuple in enumerate(input_list):
    for j, value in enumerate(tuple):
        try:
            new_val = int(float(value))
            input_list[i][j] = new_val
        except:
            ''

insert_db1(cursor, input_list, query_match)
cnxn.close()

