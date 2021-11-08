import pyodbc
from Utils import CSVtoLISTDICT, connectDB
from tqdm import tqdm

def chunks(lst, start, n):
    for i in range(start, len(lst), n):
        yield lst[i:i + n]

#Insert di più record alla volta usando execute many
def insert_db1(cursor, data, query, table, chunk_size=100):
    row_count = cursor.execute('SELECT COUNT(*) FROM ' + table).fetchall()[0][0]
    print(F'Record presenti in {table}: {row_count}')

    for chunk in tqdm(chunks(data, row_count, chunk_size)):
        try:
            cursor.executemany(query, chunk)
            cursor.commit()
        except pyodbc.IntegrityError as e:
            print(e)
            return

#Insert di un record per volta
def insert_db2(cursor, data, query, table):

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

dates = CSVtoLISTDICT('output/date.csv', True, ',')
players = CSVtoLISTDICT('output/players_noNull.csv', True, ',')
tournaments = CSVtoLISTDICT('output/tournament_noNull.csv', True, ',')
matches = CSVtoLISTDICT('output/match_noNull.csv', True, ',')
countries = CSVtoLISTDICT('output/countries.csv', True, ',')

query_dates = 'INSERT INTO Date (date_id, year, quarter, month_of_year, month, day_of_month, day, week_of_year)' \
            '            VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

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

#Insert date
input_list = [ list(date.values())  for date in dates ]
insert_db1(cursor, input_list, query_dates, 'date')

# Insert tournaments
input_list = [ list(tourney.values())  for tourney in tournaments ]
insert_db1(cursor, input_list, query_tournament, 'tournament')

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
insert_db1(cursor, input_list, query_countries, 'geography')

#Insert Players
input_list = [ list(player.values()) for player in players ]
insert_db1(cursor, input_list, query_players, 'player')

input_list = [ [ str(match['match_num']) + match['tourney_pk'] + str(match['winner_id']) + str(match['loser_id']) ] + list(match.values())[1:] for match in matches ]

insert_db1(cursor, input_list, query_match, 'match', chunk_size=250)
cnxn.close()

