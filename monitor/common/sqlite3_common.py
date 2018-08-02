import os
import re
import sys
import sqlite3

# Import openlavaMonitor packages.
if ('openlavaMonitor_development_path' in os.environ) and os.path.exists(os.environ['openlavaMonitor_development_path']):
    sys.path.insert(0, os.environ['openlavaMonitor_development_path'])

from monitor.common import common

def getSqlTableList(dbFile, curs):
    """
    Get all of the tables from the specified db file.
    """
    tableList = []

    if curs == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
            curs = conn.cursor()
        else:
            common.printError('*Error* (getSqlTableList) : "' + str(dbFile) + '" No such database file.')
            return(tableList)

    try:
        command = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        results = curs.execute(command)
        allItems = results.fetchall()
        for item in allItems:
            (key,) = item
            tableList.append(key)
        if curs == '':
            curs.close()
            conn.close()
    except Exception as error:
        common.printError('*Error* (getSqlTableList) : Failed on getting table list on dbFile "' + str(dbFile) + '": ' + str(error))

    return(tableList)

def getSqlTableKeyList(dbFile, curs, tableName):
    """
    Get all of the tables from the specified db file.
    """
    keyList = []

    if curs == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
            curs = conn.cursor()
        else:
            common.printError('*Error* (getSqlTableKeyList) : "' + str(dbFile) + '" No such database file.')
            return(keyList)

    try:
        command = "SELECT * FROM '" + str(tableName) + "'"
        curs.execute(command)
        keyList = [tuple[0] for tuple in curs.description]
        if curs == '':
            curs.close()
            conn.close()
    except Exception as error:
        common.printError('*Error* (getSqlTableKeyList) : Failed on getting table key list on dbFile "' + str(dbFile) + '": ' + str(error))

    return(keyList)

def getSqlTableData(dbFile, curs, tableName, keyList=[]):
    """
    With specified dbFile-tableName, get all data from specified keyList.
    """
    dataDic = {}

    if curs == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
            curs = conn.cursor()
        else:
            common.printError('*Error* (getSqlTableData) : "' + str(dbFile) + '" No such database file.')
            return(dataDic)

    try:
        command = "SELECT * FROM '" + str(tableName) + "'"
        results = curs.execute(command)
        allItems = results.fetchall()
        tableKeyList = [tuple[0] for tuple in curs.description]
        if curs == '':
            curs.close()
            conn.close()

        if len(keyList) == 0:
            keyList = tableKeyList
        else:
            for key in keyList:
                if key not in tableKeyList:
                    common.printError('*Error* (getSqlTableData) : "' + str(key) + '": invalid key on specified key list.')
                    return(dataDic)

        for item in allItems:
            valueList = list(item)
            for i in range(len(tableKeyList)):
                key = tableKeyList[i]
                if key in keyList:
                    value = valueList[i]
                    if key in dataDic.keys():
                        dataDic[key].append(value)
                    else:
                        dataDic[key]=[value,]
    except Exception as error:
        common.printError('*Error* (getSqlTableData) : Failed on getting table info from table "' + str(tableName) + '" of dbFile "' + str(dbFile) + '": ' + str(error))

    return(dataDic)

def dropSqlTable(dbFile, conn, tableName):
    """
    Drop table it it exists.
    """
    if conn == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
        else:
            common.printError('*Error* (dropSqlTable) : "' + str(dbFile) + '" No such database file.')
            return

    try:
        curs = conn.cursor()
        command = "DROP TABLE IF EXISTS '" + str(tableName) + "'"
        curs.execute(command)
        curs.close()
        conn.commit()
        if conn == '':
            conn.close()
    except Exception as error:
        common.printError('*Error* (dropSqlTable) : Failed on drop table "' + str(tableName) + '" from dbFile "' + str(dbFile) + '": ' + str(error))

def createSqlTable(dbFile, conn, tableName, initString):
    """
    Create a table if it not exists, initialization the setting.
    """
    if conn == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
        else:
            common.printError('*Error* (createSqlTable) : "' + str(dbFile) + '" No such database file.')
            return

    try:
        curs = conn.cursor()
        command = "CREATE TABLE IF NOT EXISTS '" + str(tableName) + "' " + str(initString)
        curs.execute(command)
        curs.close()
        conn.commit()
        if conn == '':
            conn.close()
    except Exception as error:
        common.printError('*Error* (createSqlTable) : Failed on creating table "' + str(tableName) + '" on db file "' + str(dbFile) + '": ' + str(error))

def insertIntoSqlTable(dbFile, conn, tableName, valueString):
    """
    Insert new value into sql table.
    """
    if conn == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
        else:
            common.printError('*Error* (insertIntoSqlTable) : "' + str(dbFile) + '" No such database file.')
            return

    try:
        curs = conn.cursor()
        command = "INSERT INTO '" + str(tableName) + "' VALUES " + str(valueString)
        curs.execute(command)
        curs.close()
        conn.commit()
        if conn == '':
            conn.close()
    except Exception as error:
        common.printError('*Error* (insertIntoSqlTable) : Failed on inserting specified values into table "' + str(tableName) + '" on db file "' + str(dbFile) + '": ' + str(error))

def genSqlTableKeyString(keyList):
    """
    Switch the input keyList into the sqlite table key string.
    """
    keyString = '('

    for i in range(len(keyList)):
        key = keyList[i]
        if i == 0:
            keyString = str(keyString) + "'" + str(key) + "' VARCHAR(255) PRIMARY KEY,"
        elif i == len(keyList)-1:
            keyString = str(keyString) + " '" + str(key) + "' VARCHAR(255));"
        else:
            keyString = str(keyString) + " '" + str(key) + "' VARCHAR(255),"

    return(keyString)

def genSqlTableValueString(valueList):
    """
    Switch the input valueList into the sqlite table value string.
    """
    valueString = '('

    for i in range(len(valueList)):
        value = valueList[i]
        if re.search("'", str(value)):
            value = str(value).replace("'", "''")
        if i == 0:
            valueString = str(valueString) + "'" + str(value) + "',"
        elif i == len(valueList)-1:
            valueString = str(valueString) + " '" + str(value) + "');"
        else:
            valueString = str(valueString) + " '" + str(value) + "',"

    return(valueString)
