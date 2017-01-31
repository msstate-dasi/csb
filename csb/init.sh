echo '>> Start of Script'
nodes=($( cat $PBS_NODEFILE | sort | uniq | cut -d '.' -f 1 ))
nnodes=${#nodes[@]}
last=$(( $nnodes - 2 ))

$SPARK_HOME/sbin/start-master.sh
SPARK_MASTER_IP=$HOSTNAME

echo 'Master created on $HOSTNAME'

for i in $( seq 0 $last )
do
    #ssh-copy-id ${nodes[$i]}
    ssh ${nodes[$i]} "SPARK_HOME=/work/$USER/spark $SPARK_HOME/sbin/start-slave.sh spark://$SPARK_MASTER_IP:7077"
    echo "Worker $i started on ${nodes[$i]}"
done

echo 'All Workers Started'


