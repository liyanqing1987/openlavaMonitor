import os
import re
import collections
import pexpect
import datetime
import stat

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
        if i == 0:
            keyList = line.split()
            for key in keyList:
                myDic[key] = []
        else:
            commandInfo = line.split()
            for j in range(len(commandInfo)):
                key = keyList[j]
                value = commandInfo[j]
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
                     'userCompile'                : re.compile('.*User <(\w+)>.*'),
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
    lines = os.popen(command).readlines()

    for line in lines:
        line = line.strip()

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
                warningMessage = '*Warning*: openlava have not been configured, all of the hosts are on the same queue, please update openlava configure files first.'
                printWarning(warningMessage)
                queueHostDic[queue] = getHostList()
            elif re.match('.+/', hostsString):
                groupName = re.sub('/$', '', hostsString)
                myDic = getBhostsInfo(command='bhosts ' + str(groupName))
                queueHostDic[queue] = myDic['HOST_NAME']
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
        errorMessage = '*Error*: For function "draw_plots", the length of yLists cannot be bigger than 8!'
        printError(errorMessage)
        return(1)

    colorList = ['blue', 'red', 'green', 'cyan', 'magenta', 'yellow', 'black', 'white']

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

def getSqlTableList(dbFile):
    """
    Get all of the tables from the specified db file.
    """
    tableList = []

    if not os.path.exists(dbFile):
        printError('*Error*: "' + str(dbFile) + '": No such data base file.')
    else:
        conn = sqlite3.connect(dbFile)
        curs = conn.cursor()
        command = '''SELECT name FROM sqlite_master WHERE type='type' ORDER BY name'''
        results = curs.execute(command)
        allItems = results.fetchall()
        for item in allItems:
            (time, mem) = item
            runTimeList.append(time)
            memList.append(mem)
        curs.close()
        conn.commit()
        conn.close()

    return(tableList)
