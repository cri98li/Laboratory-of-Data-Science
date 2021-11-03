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

#Ricerco i nomi dal file scaricato da http://download.geonames.org/export/dump/countryInfo.txt
for country in countries:
    if country['country_name'] in country_dict.keys():
        country['lan'] = country_dict[country['country_name']]['Language']
    else:
        country['lan'] = "" #Per le corrispondenze che non trovo assegno la stringa vuota


countriesHeader = ['country_code', 'country_name','continent','lan']
DICTtoCSV("output/countries.csv", countries, countriesHeader)




#Lettura del csv unico da rielaborare
tennis = CSVtoLISTDICT("dati/tennis.csv", True, ",")

#DATE
#Genero il nuovo valore di date nel formato AnnoQuarterMeseGiorno. Non genero un nuovo file perchè è una dimensione degenere
for row in tennis:
    data = datetime.datetime.strptime(row['tourney_date'], "%Y%m%d")
    row['date_id'] = data.strftime("%Y") + str(int((data.month + 2) / 3)) + data.strftime("%m%d")
#    row['year'] = data.year
#    row['month'] = data.month
#    row['day'] = data.day

dateHeader = ['tourney_date', 'day', 'month', 'year', 'quarter'] #date_id = tourney_date
#DICTtoCSV("output/date.csv", tennis, dateHeader)


#TOURNAMENT
for i, row in enumerate(tennis):
    if row['tourney_name'] == 'Us Open':
        tennis[i]['tourney_name'] = 'US Open'

    row["tourney_pk"] = row["tourney_id"]+row["tourney_level"]+row["date_id"]+row['tourney_name']

tournamentHeader = ["tourney_pk","tourney_id", "date_id", "tourney_name", "surface", "draw_size", "tourney_level", "tourney_spectators", "tourney_revenue"]
DICTtoCSV("output/tournament.csv", tennis, tournamentHeader)


#PLAYER
playerHeader = ["player_id", "country_id", "name", "sex", "hand", "ht", "year_of_birth"]
#male = loadNames("dati/male_players.csv")
female = loadNames("dati/female_players.csv") #carico il file più piccolo

#aggiungo il sesso e calcolo l'anno di nascita

ids_added = set()
toWrite = []
for row in tennis:
    age_days = -1

    if row['winner_id'] not in ids_added:
        ids_added.add(row["winner_id"])
        if len(row["winner_age"])>0:
            age_days = int(float(row["winner_age"]) * 365)
            matchdate = datetime.datetime.strptime(row['tourney_date'], "%Y%m%d")
            birth = matchdate - relativedelta(days=age_days)

        toWrite.append({
            "player_id": row["winner_id"],
            "country_id": row["winner_ioc"],
            "name": row["winner_name"],
            "sex": "male" if row["winner_name"] not in female else "female",
            "hand": row["winner_hand"],
            "ht": row["winner_ht"],
            "year_of_birth": birth.year if age_days != -1 else -1
        })

    age_days = -1

    if row['loser_id'] not in ids_added:
        ids_added.add(row["loser_id"])
        if len(row["loser_age"])>0:
            age_days = int(float(row["loser_age"]) * 365)
            matchdate = datetime.datetime.strptime(row['tourney_date'], "%Y%m%d")
            birth = matchdate - relativedelta(days=age_days)

        toWrite.append({
            "player_id": row["loser_id"],
            "country_id": row["loser_ioc"],
            "name": row["loser_name"],
            "sex": "male" if row["loser_name"] not in female else "female",
            "hand": row["loser_hand"],
            "ht": row["loser_ht"],
            "year_of_birth": birth.year if age_days != -1 else -1
        })

DICTtoCSV("output/players.csv", toWrite, playerHeader)


column_to_exclude = ['winner_age', 'winner_ioc', 'winner_ht', 'winner_hand', 'winner_name', 'winner_entry']\
                  + ['loser_age', 'loser_ioc', 'loser_ht', 'loser_hand', 'loser_name', 'loser_entry']


matchHeader = [x for x in tennis[0].keys() if x not in playerHeader + tournamentHeader + dateHeader + column_to_exclude] + \
                ["tourney_pk"]
DICTtoCSV("output/match.csv", tennis, matchHeader)
