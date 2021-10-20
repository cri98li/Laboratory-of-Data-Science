from Utils import CSVtoLISTDICT, DICTtoCSV

def cast(attribute=None):
    try:
        if attribute.is_integer():
            return int(attribute)
    except:
        ""
    try:
        return float(attribute)
    except:
        ""
    return attribute

def describe(data=[{}]):
    statistics = {}

    for key in data[0].keys():
        statistics[key] = {
            "min": float("+inf"),
            "min_rows": [],
            "max": float("-inf"),
            "max_rows": [],
            "avg": .0,
            "median": None,
            "distinct": set(),
            "null": 0,
            "null_rows": [],
            "null %": 0,
            "total": 0
        }

    for col in data[0].keys():
        for i, row in enumerate(data):
            value = cast(row[col])

            statistics[col]["total"] += 1

            if value is None or value == "":
                statistics[col]["null"] += 1
                statistics[col]["null_rows"].append(i)
                continue

            statistics[col]["distinct"].add(value)

            if type(value) is str:
                value = len(value)



            if statistics[col]["min"] > value:
                statistics[col]["min"] = value
                statistics[col]["min_rows"] = [i]
            elif statistics[col]["min"] == value:
                statistics[col]["min_rows"].append(i)

            if statistics[col]["max"] < value:
                statistics[col]["max"] = value
                statistics[col]["max_rows"] = [i]
            elif statistics[col]["max"] == value:
                statistics[col]["max_rows"].append(i)

            statistics[col]["avg"] += value



        statistics[col]["avg"] /= (statistics[col]["total"] - statistics[col]["null"])
        statistics[col]["null %"] = statistics[col]["null"]*100/statistics[col]["total"]
        statistics[col]["distinct"] = len(statistics[col]["distinct"])

    return statistics

def printdescribe(statistics={}, skip_rows=True):
    for attr in statistics.keys():
        print(attr, ": ")
        for stat in statistics[attr]:
            if "rows" in stat and skip_rows:
                continue
            print("\t\t{}: {}".format(stat, statistics[attr][stat]))

def fillna(data=[{}], column="", f=lambda x: x):
    for row in data:
        row[column] = f(row[column])


def dropcol(data=[{}], col_names=[]):
    for el in data:
        for col in col_names:
            el.pop(col)

def droprow(data=[{}], f=lambda x: False):
    for i, d in reversed(list(enumerate(data))):
        if f(d):
            del data[i]





match = CSVtoLISTDICT("output/match.csv", True, ",")
printdescribe(describe(match))

#score      169 drop rows
#minutes...    104347 drop rows

droprow(match, lambda x: x["score"] is None or x["score"] == "")
droprow(match, lambda x: x["minutes"] is None or x["minutes"] == "")
printdescribe(describe(match))

DICTtoCSV("output/match_noNull.csv", match, match[0].keys())


print("\n\n\n\n")


players = CSVtoLISTDICT("output/players.csv", True, ",")
printdescribe(describe(players))
#ht     136391
#hand           replace con U

dropcol(players, ["ht"])
fillna(players, "hand", lambda x: "U")

DICTtoCSV("output/players_noNull.csv", players, players[0].keys())


print("\n\n\n\n")


tournament = CSVtoLISTDICT("output/tournament.csv", True, ",")
printdescribe(describe(tournament))

#surface        62 nulli --> unknown
fillna(tournament, "surface", lambda x: "unknown")

DICTtoCSV("output/tournament_noNull.csv", tournament, tournament[0].keys())

print("\n\n\n\n")