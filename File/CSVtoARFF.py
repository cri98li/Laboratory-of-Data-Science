inputFile = open("census.csv", "r")
outputFile = open("census.arff", "w")

outputFile.write("@relation census\n\n")

matriceValoriDistinti = dict()
attributi = list()

for i, linea in enumerate(inputFile):
    linea = linea[:-1]
    valori = linea.split(",")

    for j, elemento in enumerate(valori):
        if i == 0:
            attributi = valori.copy()
            matriceValoriDistinti[attributi[j]] = set()
            continue

        matriceValoriDistinti[attributi[j]].add(elemento)


for k in matriceValoriDistinti:
    tmp = matriceValoriDistinti[k].pop()
    matriceValoriDistinti[k].add(k)
    if tmp.isdigit():
        outputFile.write("@attribute {} numeric\n".format(k))
    elif type(tmp) is str:
        outputFile.write("@attribute {} {{{}}}\n".format(k, " ".join(matriceValoriDistinti[k])).replace(" ", ", "))

    #outputFile.write("@attribute {} string\n".format(k))

outputFile.write("\n@data\n")

inputFile.seek(0)
for i, linea in enumerate(inputFile):
    if i == 0: continue;
    outputFile.write("{}".format(linea[:-1].replace(",", ", ")))
    outputFile.write("\n")

inputFile.close()
outputFile.close()