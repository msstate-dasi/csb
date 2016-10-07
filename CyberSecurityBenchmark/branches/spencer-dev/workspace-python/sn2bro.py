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

import subprocess, sys, os.path

# function takes a block and fills the neccesarry
# info then returns it is a list
def parseBlock(block):
    blockList = block.split("\n")  # get each line
    # I use the formatting of snort to fill each
    # variable with its needed info
    attackName = blockList[0] + "\n" + blockList[1]
    srcIP = blockList[2].split(" ")[1].split(":")[0]
    srcPort = blockList[2].split(" ")[1].split(":")[1]
    dstIP = blockList[2].split(" ")[3].split(":")[0]
    dstPort = blockList[2].split(" ")[3].split(":")[1]
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
        searchStr = sn_srcIP[i] + '\\t' + sn_srcPort[i] + '\\t' + sn_dstIP[i] + '\\t' + sn_dstPort[i]

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

        conns = subprocess.check_output("grep -P \"" + searchStr + "\" conn10g.log", shell=True).decode("utf-8").split(
            '\n')
        numMatches = len(conns) - 1

        if numMatches == 0:
            print("We didn't find anything for " + searchStr)
        if numMatches > 1:
            #more than one result for the info provided

            #print(str(i))
            #print(conns)
            #print(timeStamp[i])

            for line in conns:
                # If the connection was terminated before it began, no point in identifying it as bad.
                if "REJ" in line:
                    conns.remove(line)
                    numMatches = len(conns) - 1

        # For now, just grab the first stream that looks like it. This needs to be changed, perhaps to
        # add multiple streams to the output log or to identify the bad stream among good ones.
        bro_connID = conns[0].split('\t')[1]
        sn_badPacket = "True"
        sn_description = sn_attackName[i].replace('\n', '')

        # we found it!
        if bro_connID not in flaggedConns:
            bro_info = conns[0]
            bro_info = bro_info.replace('\t', ',')
            record = bro_info + ',' + sn_badPacket + ',' + sn_description

            outFile = open("sn2bro.csv", "a")
            outFile.write(record + '\n')
            outFile.close()

            # print(str(i) + '\t' + record)

            flaggedConns.append(bro_connID)
        else:
            # we already found this bad stream.
            pass

        #print(str(i) + ' ' + str(numMatches))



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

f = open(alertFile_name)  # Change this to where ever
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
    if len(line) == 1 and block.count('\n') > 5:
        print(block)
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
        
print(len(attackName))
aggregateWithBro(attackName         [:],
                 srcIP              [:],
                 srcPort            [:],
                 dstIP              [:],
                 dstPort            [:],
                 timeStamp          [:])

# Spencer this is here for you to get a look at the formating
# of these lists.  Since you need to compare each thing in here to
# attributes in bro they need to be the same.

# So with these lists you should no longer have to worry about
# the alert file.  If there is anything else I can do to help
# please email me!

#print(attackName[0])  # spans two lines
#print(srcIP[0])
#print(srcPort[0])
#print(dstIP[0])
#print(dstPort[0])
#print(timeStamp[0])
