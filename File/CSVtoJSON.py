import json
inputFile = open("data/census.csv", "r")

attributi = list()
for linea in inputFile:
    linea = linea[:-1]
    attributi = linea.split(",")
    break

jsonObj = {}
jsonObj['data'] = []
for linea in inputFile:
    linea = linea[:-1]
    valori = linea.split(",")

    innerObj = {}
    for i, attr in enumerate(attributi):
        if valori[i] == '?':
            continue
        innerObj[attr] = valori[i]

    jsonObj['data'].append(innerObj)


with open('data/cernus.json', 'w') as outfile:
    json.dump(jsonObj, outfile)