from Utils import CSVtoLISTDICT

def cast(attribute=None):
    try:
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

            if value == None or value == "":
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



match = CSVtoLISTDICT("output/match.csv", True, ",")
printdescribe(describe(match))

#score      169 drop rows
#minutes    104347 drop col


print("\n\n\n\n")


players = CSVtoLISTDICT("output/players.csv", True, ",")
printdescribe(describe(players))
#winner_hand    16 nulli
#winnder_ht     136391
#hand           replace con U
#ht             avg per sesso



print("\n\n\n\n")


tournament = CSVtoLISTDICT("output/tournament.csv", True, ",")
printdescribe(describe(tournament))

#surface        62 nulli --> unknown


print("\n\n\n\n")