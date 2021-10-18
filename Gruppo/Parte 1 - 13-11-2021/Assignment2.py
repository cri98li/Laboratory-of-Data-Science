import pyodbc
from Utils import CSVtoLISTDICT, connectDB, connectPostgre

#print(pyodbc.drivers())

cnxn = connectPostgre('cri98li.duckdns.org', 'Nomopoly', 'nomopoly', 'nomopoly2021')
cursor = cnxn.cursor()

dates = CSVtoLISTDICT('output/date.csv', True, ",")

query = 'INSERT INTO date VALUES (?)'

for date in dates:
    cursor.execute(query, date['tourney_date'])

cursor.commit()
cnxn.close()

