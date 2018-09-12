import os
import re
import sys
import sqlite3

# Import openlavaMonitor packages.
if ('openlavaMonitor_development_path' in os.environ) and os.path.exists(os.environ['openlavaMonitor_development_path']):
    sys.path.insert(0, os.environ['openlavaMonitor_development_path'])

from monitor.common import common

def connectDbFile(dbFile, mode='read'):
    result = 'passed'
    conn = ''

    if mode == 'write':
        journalDbFile = str(dbFile) + '-journal'
        if os.path.exists(journalDbFile) and (mode == 'write'):
            common.printWarning('*Warning*: database file "' + str(dbFile) + '" is on another connection, will not connect it.')
            result = 'locked'
            return(result, conn)
    elif mode == 'read':
        if not os.path.exists(dbFile):
            common.printError('*Error*: "' + str(dbFile) + '" No such database file.')
            result = 'failed'
            return(result, conn)

    try:
        conn = sqlite3.connect(dbFile)
    except Exception as error:
        common.printError('*Error*: Failed on connecting database file "' + str(dbFile) + '": ' + str(error))
        result = 'failed'

    return(result, conn)

def connectPreprocess(dbFile, orig_conn, mode='read'):
    if orig_conn == '':
        (result, conn) = connectDbFile(dbFile, mode)
    else:
        result = 'passed'
        conn = orig_conn

    curs = conn.cursor()

    return(result, conn, curs)

def getSqlTableList(dbFile, orig_conn):
    """
    Get all of the tables from the specified db file.
    """
    tableList = []

    (result, conn, curs) = connectPreprocess(dbFile, orig_conn)
    if result == 'failed':
        return(tableList)

    try:
        command = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        results = curs.execute(command)
        allItems = results.fetchall()
        for item in allItems:
            (key,) = item
            tableList.append(key)
        curs.close()
        if orig_conn == '':
            conn.close()
    except Exception as error:
        common.printError('*Error* (getSqlTableList) : Failed on getting table list on dbFile "' + str(dbFile) + '": ' + str(error))

    return(tableList)

def getSqlTableKeyList(dbFile, orig_conn, tableName):
    """
    Get all of the tables from the specified db file.
    """
    keyList = []

    (result, conn, curs) = connectPreprocess(dbFile, orig_conn)
    if result == 'failed':
        return(keyList)

    try:
        command = "SELECT * FROM '" + str(tableName) + "'"
        curs.execute(command)
        keyList = [tuple[0] for tuple in curs.description]
        curs.close()
        if orig_conn == '':
            conn.close()
    except Exception as error:
        common.printError('*Error* (getSqlTableKeyList) : Failed on getting table key list on dbFile "' + str(dbFile) + '": ' + str(error))

    return(keyList)

def getSqlTableData(dbFile, orig_conn, tableName, keyList=[]):
    """
    With specified dbFile-tableName, get all data from specified keyList.
    """
    dataDic = {}

    (result, conn, curs) = connectPreprocess(dbFile, orig_conn)
    if result == 'failed':
        return(dataDic)

    try:
        command = "SELECT * FROM '" + str(tableName) + "'"
        results = curs.execute(command)
        allItems = results.fetchall()
        tableKeyList = [tuple[0] for tuple in curs.description]
        curs.close()
        if orig_conn == '':
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

def dropSqlTable(dbFile, orig_conn, tableName, commit=True):
    """
    Drop table if it exists.
    """
    (result, conn, curs) = connectPreprocess(dbFile, orig_conn, mode='write')
    if (result == 'failed') or (result == 'locked'):
        return

    try:
        command = "DROP TABLE IF EXISTS '" + str(tableName) + "'"
        curs.execute(command)
        curs.close()
        if commit:
            conn.commit()
            if orig_conn == '':
                conn.close()
    except Exception as error:
        common.printError('*Error* (dropSqlTable) : Failed on drop table "' + str(tableName) + '" from dbFile "' + str(dbFile) + '": ' + str(error))

def createSqlTable(dbFile, orig_conn, tableName, initString, commit=True):
    """
    Create a table if it not exists, initialization the setting.
    """
    (result, conn, curs) = connectPreprocess(dbFile, orig_conn, mode='write')
    if (result == 'failed') or (result == 'locked'):
        return

    try:
        command = "CREATE TABLE IF NOT EXISTS '" + str(tableName) + "' " + str(initString)
        curs.execute(command)
        curs.close()
        if commit:
            conn.commit()
            if orig_conn == '':
                conn.close()
    except Exception as error:
        common.printError('*Error* (createSqlTable) : Failed on creating table "' + str(tableName) + '" on db file "' + str(dbFile) + '": ' + str(error))

def insertIntoSqlTable(dbFile, orig_conn, tableName, valueString, commit=True):
    """
    Insert new value into sql table.
    """
    (result, conn, curs) = connectPreprocess(dbFile, orig_conn, mode='write')
    if (result == 'failed') or (result == 'locked'):
        return

    try:
        command = "INSERT INTO '" + str(tableName) + "' VALUES " + str(valueString)
        curs.execute(command)
        curs.close()
        if commit:
            conn.commit()
            if orig_conn == '':
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
