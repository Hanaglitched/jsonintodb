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
with open('D:/my projects/jsonintodb/venv/data/json/output2.json', 'r', encoding='utf-8') as file:
    getImport = json.load(file)

# get original dataframe
df = pd.DataFrame(getImport)
uuid = df[["key"]]

# uuid insertion

# Insert DataFrame by once
val = ""
for j in range(0, len(uuid.index)):
    val += "(\"" + uuid.iloc[j]['key'] + "\")"
    if j < len(uuid.index) - 1:
        val += ","
sql_ = "INSERT INTO MAGICNOTIFY_UUID_NAME (`key`) VALUES " + val + ";"
cursor.execute(sql_)
print("uuid insertion done")


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
        cursor.execute(sql)

connection.commit()
# -----------------------------------------------

# -----------------------------------------------
# (3) get elapsed time (for test)
end_time = datetime.now()
elapsed_time = end_time - start_time
print(elapsed_time)
# -----------------------------------------------
