import csv

import pyodbc
import random

server = 'tcp:131.114.72.230'
database = 'lbi'
username = 'lds'
password = 'pisa'
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
conn = pyodbc.connect(connectionString)

#Write a code for performing a parametric select on census: one with sex = female, and one for males

cursor = conn.cursor()

cur = cursor.execute("""SELECT id, sex FROM census WHERE sex = ?""", "Female")

for row in cur.fetchall():
    print(row)

cur = cursor.execute("""SELECT id, sex FROM census WHERE sex = ?""", "Male")

for row in cur.fetchall():
    print(row)

cur.close()
conn.close()