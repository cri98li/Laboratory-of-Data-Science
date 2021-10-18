import csv
import pyodbc
import datetime
from dateutil.relativedelta import relativedelta

def CSVtoLISTDICT(filepath="", header=False, separator=','):
    data = []
    columns = []
    with open(filepath, "r") as file:
        if type(header) is bool and header:
            for column in file.readline()[:-1].split(separator):
                columns.append(column)
        elif type(header) is list:
            columns = header

        for line in file:
            row = {}
            for i, value in enumerate(line[:-1].split(separator)):
                row[columns[i]] = value
            data.append(row)

    return data

def loadNames(filepath, replace=[",", " "], skipheader=True):
    returnList = set()
    with open(filepath, "r") as file:
        for row in file:
            returnList.add(row[:-1].replace(replace[0], replace[1]))
    return returnList

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


# ------------ DB

def connectDB(server, database, username, password):
    connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    cnxn = pyodbc.connect(connectionString)

    return cnxn

def connectPostgre(server, database, username, password):
    connectionString = 'DRIVER={PostgreSQL Unicode};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password + ";PORT=5432;"
    cnxn = pyodbc.connect(connectionString)
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')
    return cnxn