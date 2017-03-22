#!/bin/bash
SNMPVERSION="2c"
SNMPCOMMUNITY="CSB"
SNMPPORT=16100

IFNAME="ib0"

CPU_USER_OID="UCD-SNMP-MIB::ssCpuUser.0"  # The percentage of CPU time spent processing user-level code, calculated over the last minute.
CPU_IDLE_OID="UCD-SNMP-MIB::ssCpuIdle.0"  # The percentage of CPU time spent idle, calculated over the last minute.
IF_NAME_OID="IF-MIB::ifName"              # The textual name of the interface.
IF_IN_OCTETS_OID="IF-MIB::ifHCInOctets"   # The total number of octets received on the interface, including framing characters.
IF_OUT_OCTETS_OID="IF-MIB::ifHCOutOctets" # The total number of octets transmitted out of the interface, including framing characters.

function snmp_get {
	snmpget -v $SNMPVERSION -c $SNMPCOMMUNITY $1:$SNMPPORT $2
}

function snmp_walk {
	snmpwalk -v $SNMPVERSION -c $SNMPCOMMUNITY $1:$SNMPPORT $2
}

function get_value {
	local string=($1)
	echo ${string[3]}
}

function get_index {
	local string=($1)
	echo ${string[0]} | cut -d. -f2
}

MASTER=$1

while [ /bin/true ]; do
	APPID=$(wget -qO- http://$MASTER:4040/api/v1/applications/  |  jq -M -r '.[0].id')
	CACHE=$(wget -qO- http://$MASTER:4040/api/v1/applications/$APPID/executors)
	NEXECUTORS=$(echo $CACHE | jq '.[] | length' | wc -l)
	NELEMENTS=$(echo $CACHE | jq '.[] | length' | head -1)

	NEXECUTORS=$(echo $NEXECUTORS-1 | bc -l)

	for i in $(seq 0 $NEXECUTORS); do
		ID=$(echo $CACHE | jq -M -r ".[$i].id")
		HOST=$(echo $CACHE | jq -M -r ".[$i].hostPort" | awk -F ':' '{print $1}')
		PORT=$(echo $CACHE | jq -M -r ".[$i].hostPort" | awk -F ':' '{print $2}')
		ISACTIVE=$(echo $CACHE | jq -M -r ".[$i].isActive")
		RDDBLOCKS=$(echo $CACHE | jq -M -r ".[$i].rddBlocks")
		MEMORYUSED=$(echo $CACHE | jq -M -r ".[$i].memoryUsed")
		DISKUSED=$(echo $CACHE | jq -M -r ".[$i].diskUsed")
		TOTALCORES=$(echo $CACHE | jq -M -r ".[$i].totalCores")
		MAXTASKS=$(echo $CACHE | jq -M -r ".[$i].maxTasks")
		ACTIVETASKS=$(echo $CACHE | jq -M -r ".[$i].activeTasks")
		FAILEDTASKS=$(echo $CACHE | jq -M -r ".[$i].failedTasks")
		COMPLETEDTASKS=$(echo $CACHE | jq -M -r ".[$i].completedTasks")
		TOTALTASKS=$(echo $CACHE | jq -M -r ".[$i].totalTasks")
		TOTALDURATION=$(echo $CACHE | jq -M -r ".[$i].totalDuration")
		TOTALGCTIME=$(echo $CACHE | jq -M -r ".[$i].totalGCTime")
		TOTALINPUTBYTES=$(echo $CACHE | jq -M -r ".[$i].totalInputBytes")
		TOTALSHUFFLEREAD=$(echo $CACHE | jq -M -r ".[$i].totalShuffleRead")
		TOTALSHUFFLEWRITE=$(echo $CACHE | jq -M -r ".[$i].totalShuffleWrite")
		MAXMEMORY=$(echo $CACHE | jq -M -r ".[$i].maxMemory")

		CPUUSER=$(get_value "$(snmp_get $HOST $CPU_USER_OID)")
		CPUIDLE=$(get_value "$(snmp_get $HOST $CPU_IDLE_OID)")
		CPUUSAGE=$((100 - $CPUIDLE))
		CPUSYSTEM=$(($CPUUSAGE - $CPUUSER))

		IFINDEX=$(get_index "$(snmp_walk $HOST $IF_NAME_OID | grep $IFNAME)")

		INBYTES=$(get_value "$(snmp_get $HOST $IF_IN_OCTETS_OID.$IFINDEX)")
		OUTBYTES=$(get_value "$(snmp_get $HOST $IF_OUT_OCTETS_OID.$IFINDEX)")

		DATE=$(date +%s)
		echo "spark.$ID.csb.host $HOST $DATE" | nc localhost 2003
		echo "spark.$ID.csb.port $PORT $DATE" | nc localhost 2003
		echo "spark.$ID.csb.isActive $ISACTIVE $DATE" | nc localhost 2003
		echo "spark.$ID.csb.rddBlocks $RDDBLOCKS $DATE" | nc localhost 2003
		echo "spark.$ID.csb.memoryUsed $MEMORYUSED $DATE" | nc localhost 2003
		echo "spark.$ID.csb.diskUsed $DISKUSED $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalCores $TOTALCORES $DATE" | nc localhost 2003
		echo "spark.$ID.csb.maxTasks $MAXTASKS $DATE" | nc localhost 2003
		echo "spark.$ID.csb.activeTasks $ACTIVETASKS $DATE" | nc localhost 2003
		echo "spark.$ID.csb.failedTasks $FAILEDTASKS $DATE" | nc localhost 2003
		echo "spark.$ID.csb.completedTasks $COMPLETEDTASKS $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalTasks $TOTALTASKS $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalDuration $TOTALDURATION $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalGCTime $TOTALGCTIME $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalInputBytes $TOTALINPUTBYTES $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalShuffleRead $TOTALSHUFFLEREAD $DATE" | nc localhost 2003
		echo "spark.$ID.csb.totalShuffleWrite $TOTALSHUFFLEWRITE $DATE" | nc localhost 2003
		echo "spark.$ID.csb.maxMemory $MAXMEMORY $DATE" | nc localhost 2003
		echo "spark.$ID.csb.appid $APPID $DATE" | nc localhost 2003

		echo "spark.$ID.csb.cpuUsage $CPUUSAGE $DATE" | nc localhost 2003
		echo "spark.$ID.csb.cpuUser $CPUUSER $DATE" | nc localhost 2003
		echo "spark.$ID.csb.cpuSystem $CPUSYSTEM $DATE" | nc localhost 2003
		echo "spark.$ID.csb.cpuIdle $CPUIDLE $DATE" | nc localhost 2003
		echo "spark.$ID.csb.cpuIdle $CPUIDLE $DATE" | nc localhost 2003

		echo "spark.$ID.csb.networkIn $INBYTES $DATE" | nc localhost 2003
		echo "spark.$ID.csb.networkOut $OUTBYTES $DATE" | nc localhost 2003

	done

	sleep 1
done


