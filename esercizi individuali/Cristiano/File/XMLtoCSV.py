outputFile = open("census_from_xml.csv", "w")

import xml.etree.ElementTree as ET
tree = ET.parse('census_row.xml')
root = tree.getroot()

str = ""
attributi = list();
for attrName in root.find("row").attrib:
    str += attrName + ","
    attributi.append(attrName)

outputFile.write(str[:-1]+"\n")

for child in root:
    str = ""
    for attrName in attributi:
        tmp = child.get(attrName)
        if tmp is None:
            str += "?,"
        else:
            str += child.get(attrName) +","

    outputFile.write(str[:-1] + "\n")