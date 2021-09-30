inputFile = open("data/customer_info.csv", "r")

joined = dict()

inputFile.readline()

for linea in inputFile:
    if "USA" not in linea:
        continue
    linea = linea[:-1]
    valori = linea.split(";")

    joined[valori[0]] = {
        "genere": valori[7],
        "sum": dict(),
        "count": dict()
    }

inputFile.close()

inputFile = open("data/sales_by_customer_month_store.csv", "r")
inputFile.readline()

for linea in inputFile:
    linea = linea[:-1]
    valori = linea.split(";")


    if valori[2] not in joined[valori[0]]["sum"]:
        joined[valori[0]]["sum"][valori[2]] = 0
        joined[valori[0]]["count"][valori[2]] = set()

    joined[valori[0]]["sum"][valori[2]] += float(valori[3])
    joined[valori[0]]["count"][valori[2]].add(valori[1])


#"idCust": {genere: F, sum: {"shopid": 123}, count: {"shopid": set()}}
result = dict()
for j_key in joined:
    for shopid in joined[j_key]["sum"]:
        r_key = str({
            "shopid": shopid,
            "genere": joined[j_key]["genere"]
        })
        if r_key not in result:
            result[r_key] = {
                "sum": 0,
                "count": set()
            }

        result[r_key]["sum"] += joined[j_key]["sum"][shopid]
        result[r_key]["count"] = result[r_key]["count"].union(joined[j_key]["count"][shopid])


for key in result:
    print(key, "\t", result[key]["sum"] / len(result[key]["count"]))