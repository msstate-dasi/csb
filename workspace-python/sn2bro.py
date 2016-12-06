# File: sn2bro.py
# Authors: Justin Lewis & Spencer Callicott
# Date: 9/21/2016
# Project: DASI
# Description: This script takes two input files, a snort alert log and a bro
#   connection log and matches the snort alerts with their respective connection
#   stream based on Source IP, Source Port, Destination IP, and Destination Port.
#   The output is placed into a file named sn2bro.csv
# Usage:
#   python sn2bro.py snort_alert_log conn.log

import subprocess, sys, os.path, time

# function takes a block and fills the necessary
# info then returns it is a list
def parseBlock(block):
    blockList = block.split("\n")  # get each line
    # I use the formatting of snort to fill each
    # variable with its needed info
    attackName = blockList[0] + "\n" + blockList[1]
    srcIP = blockList[2].split(" ")[1].split(":")[0]
    if len(blockList[2].split(" ")[1].split(":")) > 1:
        srcPort = blockList[2].split(" ")[1].split(":")[1]
    else:
        srcPort = "N/A"
    dstIP = blockList[2].split(" ")[3].split(":")[0]
    if len(blockList[2].split(" ")[3].split(":")) > 1:
        dstPort = blockList[2].split(" ")[3].split(":")[1]
    else:
        dstPort = "N/A"
    timeStamp = blockList[2].split(" ")[0]

    # i return everything in a list to add to the global list
    return [attackName, srcIP, srcPort, dstIP, dstPort, timeStamp]


def getIndices(lst, element):
    # http://stackoverflow.com/questions/6294179/how-to-find-all-occurrences-of-an-element-in-a-list
    result = []
    offset = -1
    while True:
        try:
            offset = lst.index(element, offset + 1)
        except ValueError:
            return result
        result.append(offset)


def aggregateWithBro(sn_attackName, sn_srcIP, sn_srcPort, sn_dstIP, sn_dstPort,
                     sn_timeStamp):  # you can do code here if you like
    badConnections = 0

    try:
        connFile_name = sys.argv[2]
        if not os.path.isfile(connFile_name):
            raise FileNotFoundError
    except FileNotFoundError:
        print("Error: File not Found.")
        exit()
    except:
        print("Error")
        exit()

    outFile_name = "sn2bro.csv"

    #clear out the output file if it already exists
    outFile = open(outFile_name, 'w')
    outFile.write("ts,uid,id.orig_h,id.orig_p,id.resp_h,id.resp_p,proto,service,duration,orig_bytes,resp_bytes,conn_state,local_orig,local_resp	missed_bytes,history,orig_pkts,orig_ip_bytes,resp_pkts,resp_ip_bytes,tunnel_parents,badpacket,desc\n")
    outFile.close()

    #We get a list of alerts from snort, but how many?
    numAlerts = len(sn_attackName)

    #list o' bad connections
    flaggedConns = []

    # then iterate over all of the alerts and see if we can find something that matches it.
    for i in range(numAlerts):
        #if the ports can be used then construct these search expressions
        #Note: These expressions will be fed into grep
        #Another Note: I make two expressions because sometimes the dst section and source section get swapped (BUG that needs fixing when I have time)
        if sn_srcPort[i] != "N/A":
            searchStr = ".*" + sn_srcIP[i] + '.*' + sn_srcPort[i] + '.*' + sn_dstIP[i] + '.*' + sn_dstPort[i]
            rSearchStr = ".*" + sn_dstIP[i] + '.*' + sn_dstPort[i] + '.*' + sn_srcIP[i] + '.*' + sn_srcPort[i] 
        #in the case for port scans where the port numbers vary widely then just look for the ip's
        else:
            searchStr = '.*' +  sn_srcIP[i] + '.*' + sn_dstIP[i] + '.*'
            rSearchStr = '.*' +  sn_dstIP[i] + '.*' + sn_srcIP[i] + '.*'


        # without grep
        # connFile = open(connFile_name, 'r')
        # cons = []
        # for line in connFile:
        #    if re.search(searchStr, line):
        #        conns.append(line)
        #        print(line)
        #        numMatches+=1
        # connFile.close()
        # if numMatches==0:
        #    print("Did not find search string.")


        # with grep = MUCH, MUCH FASTER


        match = list()
        try:
            conns = subprocess.check_output("grep  \"" + searchStr + "\" conn.log", shell=True).decode("utf-8").split('\n')
        except:
            conns = subprocess.check_output("grep  \"" + rSearchStr + "\" conn.log", shell=True).decode("utf-8").split('\n') 
        conns.remove("")
        #find the one line that contains the correct time stamp
        matches = 0
        for conn in conns:
            epoch = conn.split('\t')[0]
            comRange = conn.split("\t")[8]
            if comRange == '-':
                comRange = 1
            if float(timeStamp[i]) >= float(epoch) - 5 and float(timeStamp[i]) <= float(epoch) + float(comRange) + 5: #the plus and minus 5 are there mainly to catch *all* communication packets
                matches = matches + 1
                match.append(conn)
                badConnections = badConnections + 1

    

        if matches == 0:
            print("We didn't find anything for " + searchStr + " at time " + str(timeStamp[i])) #this print statement should never print but in case something goes wrong you know
        else: #we found 1 or more matches
            for oneMatch in match:
                sn_badPacket = "True"
                sn_description = sn_attackName[i].replace('\n', '')
                record = oneMatch + ',' + sn_badPacket + ',' + sn_description
                outFile = open("sn2bro.csv", "a")
                outFile.write(record + '\n')
                outFile.close()
    print("badConnections " + str(badConnections))
    subprocess.check_output("cat " + str(outFile_name) + " | sort | uniq > " + str(outFile_name), shell=True)
        
        # we found it!


######################################################## BEGIN MAIN PROGRAM

for arg in sys.argv:
    print(arg)

try:
    alertFile_name = sys.argv[1]
    if not os.path.isfile(alertFile_name):
        raise FileNotFoundError
except FileNotFoundError:
    print("Error: File not Found.")
    exit()
except:
    print("Error")
    exit()

f = open(alertFile_name)
# you put the alert file

# "global" list
# by global I mean that these lists will contain all
# attributes about every attack in the alert file
# This list is parallel meaning that each index describes
# info about the same packet
attackName = []
srcIP = []
srcPort = []
dstIP = []
dstPort = []
timeStamp = []

# I init a block and fill it in the loop
block = ""
for line in f:

    block += line
    # the block is finished when it encounters a single "\n" character
    # so grab the data out of it
    if len(line) == 1:
        #print(block)
        blockInfo = parseBlock(block)

        attackName.append(blockInfo[0])
        srcIP.append(blockInfo[1])
        srcPort.append(blockInfo[2])
        dstIP.append(blockInfo[3])
        dstPort.append(blockInfo[4])
        timeStamp.append(blockInfo[5])
        # empty block after done
        block = ""
    elif len(line) == 1:
        block = ""
attempt = "10/2/2011-12:12:10"

print(attempt)
print("flaged packets = ", end ="")
print(len(attackName))

pattern = '%m/%d/%Y-%H:%M:%S.%f'
for timeIndex in range(len(timeStamp)):
    attempt = timeStamp[timeIndex].split("\t")[0]
    attempt = attempt[:6] + "20" + attempt[6:]
    epoch = float(time.mktime(time.strptime(attempt, pattern)))
    mili = attempt.split('.')[-1]
    mili = "0." + mili
    epoch = epoch + float(mili)
    timeStamp[timeIndex] = epoch
aggregateWithBro(attackName         [:],
                 srcIP              [:],
                 srcPort            [:],
                 dstIP              [:],
                 dstPort            [:],
                 timeStamp          [:])