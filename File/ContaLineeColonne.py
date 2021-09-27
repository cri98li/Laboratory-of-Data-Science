import math

file = open("census.csv", "r")

nLinee = 0
nColonneMin = math.inf
nColonneMax = -1

for linea in file:
    nColonne = 0;
    for elemento in linea.split(","):
        if len(elemento) != 0 and elemento != '?' and elemento != "null":
            nColonne = nColonne+1
    nLinee = nLinee+1
    if(nColonneMax < nColonne):
        nColonneMax = nColonne
    if(nColonneMin > nColonne):
        nColonneMin = nColonne


print("Colonne min: {}".format(nColonneMin))
print("Colonne max: {}".format(nColonneMax))
print("\nLinee: {}".format(nLinee))

file.close()