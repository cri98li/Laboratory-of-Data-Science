import csv
import pyodbc
import random

server = 'tcp:131.114.72.230'
database = 'lbi'
username = 'lds'
password = 'pisa'
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
conn = pyodbc.connect(connectionString)
cursor = conn.cursor()

percentage = .3

selectedRows = {}

cur = cursor.execute("""SELECT sex, COUNT(*) as n
                        FROM census
                        GROUP BY sex""")

total = 0

print("Original distribution")

for el in cur.fetchall():
    selectedRows[el.sex] = {
            "samples": [],
            "nOfSelectedRecord": 0,
            "nOfOriginalRecord": 0
        }
    selectedRows[el.sex]["nOfOriginalRecord"] = el.n
    total += el.n

query = """SELECT id, sex 
            FROM census 
            WHERE sex = ?"""

totalNSamples=0

for sex in selectedRows:
    extracted = 0
    for i, row in enumerate(cur.execute(query, sex).fetchall()):
        x = int(selectedRows[sex]["nOfOriginalRecord"] * percentage - selectedRows[sex]["nOfSelectedRecord"])
        y = selectedRows[sex]["nOfOriginalRecord"] - extracted
        if int(random.uniform(0,1) * y) < x:
            #Selected
            selectedRows[sex]["samples"].append(row)
            extracted += 1
            selectedRows[sex]["nOfSelectedRecord"] += 1
        else:
            extracted += 1
    totalNSamples += extracted

print("Sample distribution")

for sex in selectedRows:
    print("Subsample of class {}:".format(sex))
    print("\t% wrt data[{}]: {}".format(sex, selectedRows[sex]["nOfSelectedRecord"]/selectedRows[sex]["nOfOriginalRecord"]))
    print("\tOriginal distribution vs subsample distribution: {} vs {}".format(selectedRows[sex]["nOfOriginalRecord"]/total, selectedRows[sex]["nOfSelectedRecord"]/totalNSamples))
    print("\t#record original vs subsample: {} vs {}".format(selectedRows[sex]["nOfOriginalRecord"], selectedRows[sex]["nOfSelectedRecord"]))

csvFile = open("es2.csv", "w", newline='')
writer = csv.writer(csvFile)
writer.writerow([x[0] for x in cur.description])

for sex in selectedRows:
    for row in selectedRows[sex]["samples"]:
        writer.writerow(row)

cur.close()
conn.close()