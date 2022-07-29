from datetime import datetime

import numpy as np
import pandas as pd
import json
import pymysql

start_time = datetime.now();
from Tools.scripts.dutree import display

# sql insert tutorial
# https://www.dataquest.io/blog/sql-insert-tutorial/
# ----------------------------------------------
# (1) database introduce area
# make connection to database
connection = pymysql.connect(host='*',
                             port=*,
                             user='*',
                             password='*',
                             db='*')
# create cursor
cursor = connection.cursor()
# -----------------------------------------------

# -----------------------------------------------
# (2) dataframe introduce area
# open json file
with open('D:/my projects/jsonintodb/venv/data/json/AllPrices.json', 'r', encoding='utf-8') as file:
    getImport = json.load(file)

# get original dataframe
df = getImport.get("data")
uuid = pd.DataFrame(df);
uuid = uuid.transpose();

# uuid insertion

# Insert DataFrame by once

print(len(uuid.index))
val = ""
# for j in range(0, len(uuid.index)):
#     val = ""
#     print(j)
#     try:
#         val = "(\"" + uuid.index[j] + "\")"
#         sql_ = "INSERT INTO MagicnotifyUuidName (`key`) VALUES " + val + ";"
#         cursor.execute(sql_)
#     except:
#         pass
# connection.commit()
print("uuid insertion done")

uuid = uuid.replace({np.nan: None})

<<<<<<< HEAD
# push datas into database, by numbers
print("b")
for i in range(11, len(uuid.index)):
    # check the progress; you could make it progress bar to get it visualize.
    print(i)
    val = ""
    key = uuid.index[i]
    if uuid.iloc[i].get("paper") != None:
        if uuid.iloc[i].get("paper").get("cardkingdom") != None:
            if uuid.iloc[i].get("paper").get("cardkingdom").get("retail") != None:
                tmp = uuid.iloc[i].get("paper").get("cardkingdom").get("retail")
                length_normal = 0
                length_foil = 0
                length = 0
                normalExistFlag = False
                foilExistFlag = False
                if tmp.get("normal") != None:
                    getNormalDict = tmp.get("normal")
                    length_normal = len(tmp.get("normal"))
                    normalExistFlag = True
                if tmp.get("foil") != None:
                    getFoilDict = tmp.get("foil")
                    length_foil = len(tmp.get("foil"))
                    foilExistFlag = True
                if length < length_normal:
                    length = length_normal
                if length < length_foil:
                    length = length_foil
                for j in range(0, length):
                    if normalExistFlag and j < length_normal:
                        date = list(getNormalDict)[j]
                        normal = str(getNormalDict[date])
                    else:
                        normal = ""
                    if foilExistFlag and j < length_foil:
                        date = list(getFoilDict)[j]
                        foil = str(getFoilDict[date])
                    else:
                        foil = ""
                    val += "(NULLIF(\"" + foil + "\",\'\') , NULLIF(\"" + normal + "\",\'\') , \"" + date + "\",\"" + key + "\")"
                    if j < length - 1:
                        val += ","
    if val != "":
        sql = "INSERT INTO MagicnotifyPrice (`foil`, `normal`, `date`, `key`) VALUES " + val + ";"
=======
# (3) push datas into database, by numbers
for i in range(0, len(df.index)):
    # check the progress; you could make it progress bar to get it visualize.
    print(i)
    val = ""
    # getValue represents Value; which will lead to foil and normal card price.
    getValue = getImport[i].get("value")
    # getKey represents Key; which is uuid made of hex numbers.
    getKey = getImport[i].get("key")

    # getFoil and getNormal represents foil and normal card prices
    getFoil = getValue.get("foil")
    getNormal = getValue.get("normal")

    # exceptional : if both price doesn't exist
    if getFoil != None or getNormal != None:
        price = pd.DataFrame(getValue)

        # getting a row name.
        # https://www.adamsmith.haus/python/answers/how-to-get-row-names-from-a-pandas-dataframe-in-python
        getIndex = price.index
        getIndexList = list(getIndex)

        # price_moda == dataframe we want to use
        price_mod = price.assign(key=getKey)
        price_moda = price_mod.assign(date=getIndexList)

        # replace NaN with None;
        # http://daplus.net/python-pandas-%EB%98%90%EB%8A%94-numpy-nan%EC%9D%84-none%EC%9C%BC%EB%A1%9C-%EB%8C%80%EC%B2%B4%ED%95%98%EC%97%AC-mysqldb%EC%99%80-%ED%95%A8%EA%BB%98-%EC%82%AC%EC%9A%A9/
        price_moda = price_moda.replace({np.nan: None})
        # price insertion
        for j in range(0, len(price_moda.index)):
            # introducing variables
            foil = str(price_moda.iloc[j]['foil'])
            normal = str(price_moda.iloc[j]['normal'])
            date = str(price_moda.iloc[j]['date'])
            key = str(price_moda.iloc[j]['key'])

            val += "(\"" + foil + "\",\"" + normal + "\",\"" + date + "\",\"" + key + "\")"
            if j < len(price_moda.index) - 1:
                val += ","
        sql = "INSERT INTO MAGICNOTIFY_PRICE (`foil`, `normal`, `date`, `key`) VALUES " + val + ";"
>>>>>>> b0eb01f1ea2d85b00e80642e55970aade4d6ce0c
        cursor.execute(sql)

connection.commit()
# -----------------------------------------------

# -----------------------------------------------
# (3) get elapsed time (for test)
end_time = datetime.now()
elapsed_time = end_time - start_time
print(elapsed_time)
# -----------------------------------------------
