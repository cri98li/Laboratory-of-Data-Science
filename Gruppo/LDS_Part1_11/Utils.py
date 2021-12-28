import csv
import pyodbc
import datetime
from tqdm import tqdm
from dateutil.relativedelta import relativedelta

#Legge un file csv (o qualsiasi file con valori separati da un carattere o stringa) come lista di dizionari
#Il valore di header può essere sia una lista che contiene i nomi, ordinati, delle colonne o un booleano
def CSVtoLISTDICT(filepath="", header=False, separator=',', lower=False):
    data = []
    columns = []
    with open(filepath, "r") as file:
        if type(header) is bool and header:
            for column in file.readline()[:-1].split(separator):
                columns.append(column)
        elif type(header) is list:
            columns = header

        for line in file:
            if lower:
                line = line.lower()
            row = {}
            for i, value in enumerate(line.replace("\n", "").split(separator)):
                row[columns[i]] = value
            data.append(row)

    return data

#Usata per caricare i nomi dei giocatori per estrarre il genere
def loadNames(filepath, replace=[",", " "], skipheader=True):
    returnList = set()
    with open(filepath, "r") as file:
        for row in file:
            returnList.add(row[:-1].replace(replace[0], replace[1]))
    return returnList

#Usata per salvare la lista di dizionari su un file. Elimina anche i dizionari duplicati
def DICTtoCSV(filepath="", data=[{}], header=True,):
    with open(filepath, "w", newline='') as file:
        keys = data[0].keys()
        if type(header) is list:
            keys = header

        fileCSV = csv.DictWriter(file, keys)
        toWrite = map(lambda row: {key: row[key] for key in keys}, data)
        fileCSV.writeheader()
        toWrite = [dict(t) for t in {tuple(d.items()) for d in toWrite}] #Elimina i duplicati
        fileCSV.writerows(toWrite)


def map_continent(x):
    if(x == 'AF'):
        return 'Africa'
    elif(x == 'AS'):
        return 'Asia'
    elif(x == 'EU'):
        return 'Europe'
    elif(x == 'NA'):
        return 'America'
    else:
        return 'Unkwown'

# ------------ DB

def connectDB(server, database, username, password):
    connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    cnxn = pyodbc.connect(connectionString)

    return cnxn


def chunks(lst, start, n):
    for i in range(start, len(lst), n):
        yield lst[i:i + n]

#Insert di più record alla volta usando execute many
def insert_db1(cursor, data, query, table, chunk_size=300):
    row_count = cursor.execute('SELECT COUNT(*) FROM ' + table).fetchall()[0][0]
    print(F'\r\nRecord già presenti nella tabella "{table}": {row_count}. Da inserire {len(data) - row_count}')

    for chunk in tqdm(chunks(data, row_count, chunk_size)):
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