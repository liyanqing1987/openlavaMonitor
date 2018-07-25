import os
import re
import collections
import pexpect
import stat
import sqlite3
import subprocess

def printError(message):
    """
    Print error message with red color.
    """
    print('\033[1;31m' + str(message) + '\033[0m')

def printWarning(message):
    """
    Print warning message with yellow color.
    """
    print('\033[1;33m' + str(message) + '\033[0m')

def getCommandDict(command):
    """
    Collect (common) openlava command info into a dict.
    It only works with the Title-Item type informations.
    """
    myDic = collections.OrderedDict()
    keyList = []
    lines = os.popen(command).readlines()

    for i in range(len(lines)):
        line = lines[i].strip()

        # Some speciall preprocess.
        if re.search('lsload', command):
            line = re.sub('\*', ' ', line)

        if i == 0:
            keyList = line.split()
            for key in keyList:
                myDic[key] = []
        else:
            commandInfo = line.split()
            if len(commandInfo) < len(keyList):
                printWarning('*Warning* (getCommandDict) : For command "' + str(command) + '", below info line is incomplate/unexpected.')
                printWarning('           ' + str(line))

            for j in range(len(keyList)):
                key = keyList[j]
                if j < len(commandInfo):
                    value = commandInfo[j]
                else:
                    value = ''
                myDic[key].append(value)

    return(myDic)

def getBqueuesInfo(command='bqueues -w'):
    """
    Get bqueues info with command 'bqueues'.
    ====
    QUEUE_NAME     PRIO      STATUS      MAX  JL/U JL/P JL/H NJOBS  PEND  RUN  SUSP
    normal          30    Open:Active      -    -    -    -     1     0     1     0
    ====
    """
    bqueuesDic = getCommandDict(command)
    return(bqueuesDic)

def getBhostsInfo(command='bhosts -w'):
    """
    Get bhosts info with command 'bhosts'.
    ====
    HOST_NAME          STATUS       JL/U    MAX  NJOBS    RUN  SSUSP  USUSP    RSV 
    lavaHost1          ok              -      2      1      1      0      0      0
    ====
    """
    bhostsDic = getCommandDict(command)
    return(bhostsDic)

def getLshostsInfo(command='lshosts -w'):
    """
    Get lshosts info with command 'lshosts'.
    ====
    HOST_NAME      type    model  cpuf ncpus maxmem maxswp server RESOURCES
    lavaHost1     linux  IntelI5 100.0     2  7807M  5119M    Yes (cs)
    ====
    """
    lshostsDic = getCommandDict(command)
    return(lshostsDic)

def getLsloadInfo(command='lsload -w'):
    """
    Get lsload info with command 'lsload'.
    ====
    HOST_NAME       status  r15s   r1m  r15m   ut    pg  ls    it   tmp   swp   mem
    lavaHost1           ok   0.3   0.1   0.1  19%   0.0   3     5   35G 5120M 6688M
    ====
    """
    lsloadDic = getCommandDict(command)

    return(lsloadDic)

def getBusersInfo(command='busers all'):
    """
    Get lsload info with command 'busers'.
    ====
    USER/GROUP          JL/P    MAX  NJOBS   PEND    RUN  SSUSP  USUSP    RSV 
    yanqing.li             -      -      0      0      0      0      0      0
    ====
    """
    busersDic = getCommandDict(command)
    return(busersDic)

def getBjobsUfInfo(command='bjobs -u all -r -UF'):
    """
    Parse job info which are from command 'bjobs -u all -r -UF'.
    ====
    Job <205>, User <liyanqing>, Project <default>, Status <PEND>, Queue <normal>, Command <sleep 1000>
    Sun May 13 18:08:26: Submitted from host <lavaHost1>, CWD <$HOME>, 2 Processors Requested, Requested Resources <rusage[mem=1234] span[hosts=1]>;
    PENDING REASONS:
    New job is waiting for scheduling: 1 host;
    
    SCHEDULING PARAMETERS:
              r15s   r1m  r15m   ut      pg    io   ls    it    tmp    swp    mem
    loadSched   -     -     -     -       -     -    -     -     -      -      -  
    loadStop    -     -     -     -       -     -    -     -     -      -      -  
    
    RESOURCE REQUIREMENT DETAILS:
    Combined: rusage[mem=1234] span[hosts=1]
    Effective: rusage[mem=1234] span[hosts=1]
    ====
    """
    jobCompileDic = {
                     'jobCompile'                 : re.compile('.*Job <([0-9]+(\[[0-9]+\])?)>.*'),
                     'jobNameCompile'             : re.compile('.*Job Name <([^>]+)>.*'),
                     'userCompile'                : re.compile('.*User <([^>]+)>.*'),
                     'projectCompile'             : re.compile('.*Project <([^>]+)>.*'),
                     'statusCompile'              : re.compile('.*Status <([A-Z]+)>*'),
                     'queueCompile'               : re.compile('.*Queue <([^>]+)>.*'),
                     'commandCompile'             : re.compile('.*Command <([^>]+)>.*'),
                     'submittedFromCompile'       : re.compile('.*Submitted from host <([^>]+)>.*'),
                     'submittedTimeCompile'       : re.compile('(.*): Submitted from host.*'),
                     'cwdCompile'                 : re.compile('.*CWD <([^>]+)>.*'),
                     'processorsRequestedCompile' : re.compile('.* ([1-9][0-9]*) Processors Requested.*'),
                     'requestedResourcesCompile'  : re.compile('.*Requested Resources <(.+)>;.*'),
                     'spanHostsCompile'           : re.compile('.*Requested Resources <.*span\[hosts=([1-9][0-9]*).*>.*'),
                     'rusageMemCompile'           : re.compile('.*Requested Resources <.*rusage\[mem=([1-9][0-9]*).*>.*'),
                     'startedOnCompile'           : re.compile('.*[sS]tarted on ([0-9]+ Hosts/Processors )?([^;,]+).*'),
                     'startedTimeCompile'         : re.compile('(.*): [sS]tarted on.*'),
                     'cpuTimeCompile'             : re.compile('.*The CPU time used is ([1-9][0-9]*) seconds.*'),
                     'memCompile'                 : re.compile('.*MEM: ([1-9][0-9]*) Mbytes.*'),
                    }

    myDic = collections.OrderedDict()
    job = ''
    #lines = os.popen(command).readlines()

    p = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lines = p.stdout.readlines()

    for line in lines:
        line = str(line.strip(), 'utf-8')

        if re.match('Job <' + str(job) + '> is not found', line):
            continue
        else:
            if jobCompileDic['jobCompile'].match(line):
                myMatch = jobCompileDic['jobCompile'].match(line)
                job = myMatch.group(1)

                # Initialization for myDic[job].
                myDic[job] = collections.OrderedDict()
                myDic[job]['jobId'] = job
                myDic[job]['jobName'] = ''
                myDic[job]['user'] = ''
                myDic[job]['project'] = ''
                myDic[job]['status'] = ''
                myDic[job]['queue'] = ''
                myDic[job]['command'] = ''
                myDic[job]['submittedFrom'] = ''
                myDic[job]['submittedTime'] = ''
                myDic[job]['cwd'] = ''
                myDic[job]['processorsRequested'] = ''
                myDic[job]['requestedResources'] = ''
                myDic[job]['spanHosts'] = ''
                myDic[job]['rusageMem'] = ''
                myDic[job]['startedOn'] = ''
                myDic[job]['startedTime'] = ''
                myDic[job]['cpuTime'] = ''
                myDic[job]['mem'] = ''

            if job != '':
                if 'jobInfo' in myDic[job].keys():
                    myDic[job]['jobInfo'] = str(myDic[job]['jobInfo']) + '\n' + str(line)
                else:
                    myDic[job]['jobInfo'] = line

                if jobCompileDic['jobNameCompile'].match(line):
                    myMatch = jobCompileDic['jobNameCompile'].match(line)
                    myDic[job]['jobName'] = myMatch.group(1)
                if jobCompileDic['userCompile'].match(line):
                    myMatch = jobCompileDic['userCompile'].match(line)
                    myDic[job]['user'] = myMatch.group(1)
                if jobCompileDic['projectCompile'].match(line):
                    myMatch = jobCompileDic['projectCompile'].match(line)
                    myDic[job]['project'] = myMatch.group(1)
                if jobCompileDic['statusCompile'].match(line):
                    myMatch = jobCompileDic['statusCompile'].match(line)
                    myDic[job]['status'] = myMatch.group(1)
                if jobCompileDic['queueCompile'].match(line):
                    myMatch = jobCompileDic['queueCompile'].match(line)
                    myDic[job]['queue'] = myMatch.group(1)
                if jobCompileDic['commandCompile'].match(line):
                    myMatch = jobCompileDic['commandCompile'].match(line)
                    myDic[job]['command'] = myMatch.group(1)
                if jobCompileDic['submittedFromCompile'].match(line):
                    myMatch = jobCompileDic['submittedFromCompile'].match(line)
                    myDic[job]['submittedFrom'] = myMatch.group(1)
                if jobCompileDic['submittedTimeCompile'].match(line):
                    myMatch = jobCompileDic['submittedTimeCompile'].match(line)
                    myDic[job]['submittedTime'] = myMatch.group(1)
                if jobCompileDic['cwdCompile'].match(line):
                    myMatch = jobCompileDic['cwdCompile'].match(line)
                    myDic[job]['cwd'] = myMatch.group(1)
                if jobCompileDic['processorsRequestedCompile'].match(line):
                    myMatch = jobCompileDic['processorsRequestedCompile'].match(line)
                    myDic[job]['processorsRequested'] = myMatch.group(1)
                if jobCompileDic['requestedResourcesCompile'].match(line):
                    myMatch = jobCompileDic['requestedResourcesCompile'].match(line)
                    myDic[job]['requestedResources'] = myMatch.group(1)
                if jobCompileDic['spanHostsCompile'].match(line):
                    myMatch = jobCompileDic['spanHostsCompile'].match(line)
                    myDic[job]['spanHosts'] = myMatch.group(1)
                if jobCompileDic['rusageMemCompile'].match(line):
                    myMatch = jobCompileDic['rusageMemCompile'].match(line)
                    myDic[job]['rusageMem'] = myMatch.group(1)
                if jobCompileDic['startedOnCompile'].match(line):
                    myMatch = jobCompileDic['startedOnCompile'].match(line)
                    startedHost = myMatch.group(2)
                    startedHost = re.sub('<', '', startedHost)
                    startedHost = re.sub('>', '', startedHost)
                    myDic[job]['startedOn'] = startedHost
                if jobCompileDic['startedTimeCompile'].match(line):
                    myMatch = jobCompileDic['startedTimeCompile'].match(line)
                    myDic[job]['startedTime'] = myMatch.group(1)
                if jobCompileDic['cpuTimeCompile'].match(line):
                    myMatch = jobCompileDic['cpuTimeCompile'].match(line)
                    myDic[job]['cpuTime'] = myMatch.group(1)
                if jobCompileDic['memCompile'].match(line):
                    myMatch = jobCompileDic['memCompile'].match(line)
                    myDic[job]['mem'] = myMatch.group(1)

    return(myDic)
 
def getHostList():
    """
    Get all of the hosts.
    """
    bhostsDic = getBhostsInfo()
    hostList = bhostsDic['HOST_NAME']
    return(hostList)

def getQueueList():
    """
    Get all of the queues.
    """
    bqueuesDic = getBqueuesInfo()
    queueList = bqueuesDic['QUEUE_NAME']
    return(queueList)

def getHostGroupMembers(hostGroupName):
    """
    Get host group members with bmgroup.
    ====
    [yanqing.li@nxnode03 openlavaMonitor]$ bmgroup pd
    GROUP_NAME    HOSTS
    pd           dm006 dm007 dm010 dm009 dm002 dm003 dm005 
    ====
    """
    hostList = []
    lines = os.popen('bmgroup ' + str(hostGroupName)).readlines()

    for line in lines:
        if re.match('^' + str(hostGroupName) + ' .*$', line):
            myList = line.split()
            hostList = myList[1:]

    return(hostList)

def getUserGroupMembers(userGroupName):
    """
    Get user group members with bugroup.
    ====
    [yanqing.li@nxnode03 openlavaMonitor]$ bugroup pd
    GROUP_NAME    USERS
    pd           yanqing.li san.zhang si.li
    ====
    """
    userList = []
    lines = os.popen('bugroup ' + str(userGroupName)).readlines()

    for line in lines:
        if re.match('^' + str(userGroupName) + ' .*$', line):
            myList = line.split()
            userList = myList[1:]

    return(userList)

def getQueueHostInfo():
    """
    Get hosts on (specified) queues.
    """
    queueHostDic = {}
    queueCompile = re.compile('^QUEUE:\s*(\S+)\s*$')
    hostsCompile= re.compile('^HOSTS:\s*(.*?)\s*$')
    queue = ''

    lines = os.popen('bqueues -l').readlines()
    for line in lines:
        line = line.strip()
        if queueCompile.match(line):
            myMatch = queueCompile.match(line)
            queue = myMatch.group(1)
            queueHostDic[queue] = []
        if hostsCompile.match(line):
            myMatch = hostsCompile.match(line)
            hostsString = myMatch.group(1)
            if re.search('all hosts used by the OpenLava system', hostsString):
                printWarning('*Warning* (getQueueHostInfo) : queue "' + str(queue) + '" is not well configured, all of the hosts are on the same queue.')
                queueHostDic[queue] = getHostList()
            elif re.match('.+/', hostsString):
                hostGroupName = re.sub('/$', '', hostsString)
                queueHostDic[queue] = getHostGroupMembers(hostGroupName)
            else:
                queueHostDic[queue] = hostsString.split()

    return(queueHostDic)

def getHostQueueInfo():
    """
    Get queues which (specified) host belongs to.
    """
    hostQueueDic = {}

    queueHostDic = getQueueHostInfo()
    queueList = list(queueHostDic.keys())

    for queue in queueList:
        hostList = queueHostDic[queue]
        for host in hostList:
            if host in hostQueueDic.keys():
               hostQueueDic[host].append(queue)
            else:
                hostQueueDic[host] = [queue, ]

    return(hostQueueDic)

def getRemoteProcessInfo(hostName, userName, password):
    """
    Get process info on the specified host.
    The userName must be a Privileged account, so it can ssh all of the openlava hosts.
    """
    processDic = collections.OrderedDict()

    # Login specified host with specified userName and password, all process info.
    command = 'ssh -tt ' + str(hostName) + ' -l ' + str(userName) + " 'ps aux'"
    try:
        child = pexpect.spawn(command, timeout=20)
        returnCode = child.expect("Are you sure you want to continue connecting (yes/no)?", timeout=5)
        if returnCode == 0:
            child.sendline('yes')
    except:
        pass

    child.expect(str(userName) + '@' + str(hostName) + "'s password:")
    child.sendline(password)
    child.expect(pexpect.EOF)

    output = str(child.before, encoding='utf-8')
    lines = output.split('\n')

    for i in range(len(lines)):
        line = lines[i]
        if i == 0:
            keyList = line.split()
            for key in keyList:
                processDic[key] = []
        else:
            processInfo = line.split()
            for j in range(len(processInfo)):
                key = keyList[j]
                value = processInfo[j]
                processDic[key].append(value)

    return(processDic)

def openlavaDebug(debugString):
    """
    If os.environ["openlavadebug"] have been set, print the specified debug string.
    """
    if "openlavadebug" in os.environ:
        print('[DEBUG] ' + str(debugString))

def stringToInt(inputString):
    """
    Switch the input string into ASCII number.
    """
    intNum = ''
    for char in inputString:
        num = ord(char)
        intNum = str(intNum) + str(num)
    intNum = int(intNum)
    return(intNum)

def drawPlot(xList, yList, xLabel, yLabel, xIsString=False, yUnit='', title='', saveName='', figureNum=1):
    """
    Draw a curve with pyplot.
    """
    from matplotlib import pyplot

    fig = pyplot.figure(figureNum)

    # Draw the pickture.
    if xIsString:
        xStringList = xList
        xList = range(len(xList))
        pyplot.xticks(xList, xStringList, rotation=30, fontsize=12)
        pyplot.plot(yList, 'ro-')
    else:
        pyplot.plot(xList, yList, 'ro-')

    pyplot.xlabel(xLabel)
    pyplot.ylabel(yLabel)

    pyplot.grid(True)
    pyplot.subplots_adjust(bottom=0.15)

    # Set title.
    if title != '':
        pyplot.title(title)

    # Get value info.
    xMin = min(xList)
    xMax = max(xList)
    yMin = min(yList)
    yMax = max(yList)

    # Define the curve range.
    if len(xList) == 1:
        pyplot.xlim(xMin-1, xMax+1)
        pyplot.ylim(yMin-1, yMax+1)
    else:
        pyplot.xlim(xMin, xMax)
        if yMin == yMax:
            pyplot.ylim(yMin-1, yMax+1)
        else:
            pyplot.ylim(1.1*yMin-0.1*yMax, 1.1*yMax-0.1*yMin)

    # Show the hight/avrage/low value.
    pyplot.text(xMin, yMax, 'peak: ' + str(yMax) + str(yUnit))

    # Save fig, or show it.
    if saveName != '':
        fig.savefig(saveName)
        os.chmod(saveName, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
    else:
        fig.show()

def drawPlots(xList, yLists, xLabel, yLabel, yLabels, xIsString=False, title='', saveName='', figureNum=1):
    """
    Draw a curve with pyplot.
    """
    from matplotlib import pyplot

    fig = pyplot.figure(figureNum)

    if len(yLists) > 8:
        printError('*Error* (drawPlots) : For function "draw_plots", the length of yLists cannot be bigger than 8!')
        return(1)

    colorList = ['red', 'green', 'yellow', 'cyan', 'magenta', 'blue', 'black', 'white']

    # Draw the pickture.
    if xIsString:
        xStringList = xList
        xList = range(len(xList))
        pyplot.xticks(xList, xStringList, rotation=30, fontsize=12)
        for i in range(len(yLists)):
            pyplot.plot(yLists[i], color=colorList[i], label=yLabels[i], linestyle='-')
    else:
        for i in range(len(yLists)):
            pyplot.plot(xList, yLists[i], color=colorList[i], label=yLabels[i], linestyle='-')

    pyplot.legend(loc='upper right')

    pyplot.xlabel(xLabel)
    pyplot.ylabel(yLabel)

    pyplot.grid(True)
    pyplot.subplots_adjust(bottom=0.15)

    # Set title.
    if title != '':
        pyplot.title(title)

    # Save fig, or show it.
    if saveName != '':
        fig.savefig(saveName)
        os.chmod(saveName, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
    else:
        fig.show()

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
            printError('*Error* (getSqlTableList) : "' + str(dbFile) + '" No such database file.')
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
        printError('*Error* (getSqlTableList) : Failed on getting table list on dbFile "' + str(dbFile) + '".')

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
            printError('*Error* (getSqlTableKeyList) : "' + str(dbFile) + '" No such database file.')
            return(keyList)

    try:
        command = "SELECT * FROM '" + str(tableName) + "'"
        curs.execute(command)
        keyList = [tuple[0] for tuple in curs.description]
        if curs == '':
            curs.close()
            conn.close()
    except Exception as error:
        printError('*Error* (getSqlTableKeyList) : Failed on getting table key list on dbFile "' + str(dbFile) + '".')

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
            printError('*Error* (getSqlTableData) : "' + str(dbFile) + '" No such database file.')
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
                    printError('*Error* (getSqlTableData) : "' + str(key) + '": invalid key on specified key list.')
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
        printError('*Error* (getSqlTableData) : Failed on getting table info from table "' + str(tableName) + '" of dbFile "' + str(dbFile) + '".')

    return(dataDic)

def dropSqlTable(dbFile, conn, tableName):
    """
    Drop table it it exists.
    """
    if conn == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
        else:
            printError('*Error* (dropSqlTable) : "' + str(dbFile) + '" No such database file.')
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
        printError('*Error* (dropSqlTable) : Failed on drop table "' + str(tableName) + '" from dbFile "' + str(dbFile) + '".')

def createSqlTable(dbFile, conn, tableName, initString):
    """
    Create a table if it not exists, initialization the setting.
    """
    if conn == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
        else:
            printError('*Error* (createSqlTable) : "' + str(dbFile) + '" No such database file.')
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
        printError('*Error* (createSqlTable) : Failed on creating table "' + str(tableName) + '" on db file "' + str(dbFile) + '": ' + str(error))

def insertIntoSqlTable(dbFile, conn, tableName, valueString):
    """
    Insert new value into sql table.
    """
    if conn == '':
        if os.path.exists(dbFile):
            conn = sqlite3.connect(dbFile)
        else:
            printError('*Error* (insertIntoSqlTable) : "' + str(dbFile) + '" No such database file.')
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
        printError('*Error* (insertIntoSqlTable) : Failed on inserting specified values into table "' + str(tableName) + '" on db file "' + str(dbFile) + '": ' + str(error))

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
