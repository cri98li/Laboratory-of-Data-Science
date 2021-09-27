inputFile = open("census.csv", "r")
outputFile = open("census_selected.csv", "w")

selectedColumns = [1,3]

for linea in inputFile:
    nColonne = 0
    firstElement = True
    for i, elemento in enumerate(linea.split(",")):
        if i in selectedColumns:
            if not firstElement:
                outputFile.write(',')
            outputFile.write(elemento)
            firstElement = False

    outputFile.write("\n")

inputFile.close()
outputFile.close()