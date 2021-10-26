import csv
import datetime, json
from dateutil.relativedelta import relativedelta
from Utils import CSVtoLISTDICT, loadNames, DICTtoCSV

#COUNTRIES
countries = CSVtoLISTDICT("dati/countries.csv", True, ",")

country_list = CSVtoLISTDICT("dati/countryInfo.tsv", True, "\t")
country_dict = {x['Country']: {
                                #"code": x['ISO3'],
                                #"continent": x['Continent'],
                                "Language": x['Languages'].lower().split(",")[0]
                            }  for x in country_list}

for country in countries:
    if country['country_name'] in country_dict.keys():
        country['lan'] = country_dict[country['country_name']]['Language']
    else:
        country['lan'] = ""


countriesHeader = ['country_name','continent','lan']
DICTtoCSV("output/countries.csv", countries, countriesHeader)




tennis = CSVtoLISTDICT("dati/tennis.csv", True, ",")

#DATE
#Aggiungo ad ogni riga le info sulla data
for row in tennis:
    data = datetime.datetime.strptime(row['tourney_date'], "%Y%m%d")
    row['date_id'] = data.strftime("%Y") + str(int((data.month + 2) / 3)) + data.strftime("%m%d")
#    row['year'] = data.year
#    row['month'] = data.month
#    row['day'] = data.day

dateHeader = ['tourney_date', 'day', 'month', 'year', 'quarter'] #date_id = tourney_date
#DICTtoCSV("output/date.csv", tennis, dateHeader)


#TOURNAMENT
tournamentHeader = ["tourney_id", "date_id", "tourney_name", "surface", "draw_size", "tourney_level", "tourney_spectators", "tourney_revenue"]
DICTtoCSV("output/tournament.csv", tennis, tournamentHeader)


#PLAYER
playerHeader = ["player_id", "country_id", "name", "sex", "hand", "ht", "year_of_birth"]
#male = loadNames("dati/male_players.csv")
female = loadNames("dati/female_players.csv") #carico il più piccolo

#aggiungo il sesso
winner_id = set(map(lambda row: row['winner_id'], tennis))
loser_id = set(map(lambda row: row['loser_id'], tennis))

ids_added = set()
toWrite = []
for row in tennis:
    if row['winner_id'] not in ids_added and len(row["winner_age"])>0:
        ids_added.add(row["winner_id"])
        age_d = int(float(row["winner_age"]) * 365)
        matchdate = datetime.datetime.strptime(row['tourney_date'],
                                               "%Y%m%d")  ### Posso usare anche le variabili create prima...
        birth = matchdate - relativedelta(days=age_d)
        toWrite.append({
            "player_id": row["winner_id"],
            "country_id": row["winner_ioc"],
            "name": row["winner_name"],
            "sex": "male" if row["winner_name"] not in female else "female",
            "hand": row["winner_hand"],
            "ht": row["winner_ht"],
            "year_of_birth": birth.year
        })

    if row['loser_id'] not in ids_added and len(row["loser_age"])>0:
        ids_added.add(row["loser_id"])
        age_d = int(float(row["loser_age"]) * 365)
        matchdate = datetime.datetime.strptime(row['tourney_date'],
                                               "%Y%m%d")
        birth = matchdate - relativedelta(days=age_d)
        toWrite.append({
            "player_id": row["loser_id"],
            "country_id": row["loser_ioc"],
            "name": row["loser_name"],
            "sex": "male" if row["loser_name"] not in female else "female",
            "hand": row["loser_hand"],
            "ht": row["loser_ht"],
            "year_of_birth": birth.year
        })

DICTtoCSV("output/players.csv", toWrite, playerHeader)


column_to_exclude = ['winner_age', 'winner_ioc', 'winner_ht', 'winner_hand', 'winner_name', 'winner_entry']\
                  + ['loser_age', 'loser_ioc', 'loser_ht', 'loser_hand', 'loser_name', 'loser_entry']


matchHeader = [x for x in tennis[0].keys() if x not in playerHeader + tournamentHeader + dateHeader + column_to_exclude]
DICTtoCSV("output/match.csv", tennis, matchHeader)
