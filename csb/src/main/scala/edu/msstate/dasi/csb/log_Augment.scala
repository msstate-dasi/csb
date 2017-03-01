package edu.msstate.dasi.csb

import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

/**
  * Created by B1nary on 12/8/2016.
  */
class log_Augment extends Serializable {

  private case class alertBlock(
                         attackName: String = "",
                         srcIP: String = "",
                         srcPort: Int = 0,
                         dstIP: String = "",
                         dstPort: Int = 0,
                         timeStamp: String = ""
                       )

  private case class connLogEntry(TS: String = "",
                          UID: String = "",
                          SRCADDR: String = "",
                          SRCPORT: Int = 0,
                          DESTADDR: String = "",
                          DESTPORT: Int = 0,
                          PROTOCOL: String = "",
                          SERVICE: String = "-",
                          DURATION: Double = 0,
                          ORIG_BYTES: Long = 0,
                          RESP_BYTES: Long = 0,
                          CONN_STATE: String = "",
                          LOCAL_ORIG: String = "-",
                          LOCAL_RESP: String = "-",
                          MISSED_BYTES: Long = 0,
                          HISTORY: String = "",
                          ORIG_PKTS: Long = 0,
                          ORIG_IP_BYTES: Long = 0,
                          RESP_PKTS: Long = 0,
                          RESP_IP_BYTES: Long = 0,
                          TUNNEL_PARENT: String = "(empty)",
                          DESC: String = ""
                         )

  private def getDate(in: Array[String]): String = {
    val pattern = "MM/dd/yy-HH:mm:ss"
    val dateFormatter = new SimpleDateFormat(pattern)
    //TODO the following can be refactored as split(regexp)
    val dateTime = dateFormatter.parse(in(2).split(" ")(0).split('.').head)
    dateTime.getTime.toString
  }

  private def getSnortAlertInfo(alertLog: String): RDD[alertBlock] = {
    println(sc.wholeTextFiles(alertLog).flatMap(x => x._2.split("\n\n")).count())

    sc.wholeTextFiles(alertLog).flatMap(x => x._2.split("\n\n")).map { block =>
      //try {
      val blockList = block.split("\n")
      val attackName = blockList(0) + " " + blockList(1)

      val srcIP = blockList(2).split(" ")(1).split(":")(0)
      var srcPort = 0
      if (blockList(2).split(" ")(1).split(":").length > 1)
        srcPort = blockList(2).split(" ")(1).split(":")(1).toInt

      val dstIP = blockList(2).split(" ")(3).split(":")(0)
      var dstPort = 0
      if (blockList(2).split(" ")(3).split(":").length > 1)
        dstPort = blockList(2).split(" ")(3).split(":")(1).toInt

      //println(blockList(2).split(" ")(0))
      val timeStamp = getDate(blockList)

      alertBlock(attackName, srcIP, srcPort, dstIP, dstPort, timeStamp)
      //} catch {
      //case _ => alertBlock()
      //}
    }.filter(_.timeStamp != "")
  }

  private def getBroLogInfo(connLog: String): RDD[connLogEntry] = {
    sc.textFile(connLog).map { line =>
      try {
        if (line.contains("#")) connLogEntry()

        val fields = line.split("\t")

        val TS: String = fields(0)
        val UID: String = fields(1)
        val SRCADDR: String = fields(2)
        val SRCPORT: Int = fields(3).toInt
        val DESTADDR: String = fields(4)
        val DESTPORT: Int = fields(5).toInt
        val PROTOCOL: String = fields(6)
        val SERVICE: String = fields(7)
        val DURATION: Double = fields(8).toDouble
        val ORIG_BYTES: Long = fields(9).toLong
        val RESP_BYTES: Long = fields(10).toLong
        val CONN_STATE: String = fields(11)
        val LOCAL_ORIG: String = fields(12)
        val LOCAL_RESP: String = fields(13)
        val MISSED_BYTES: Long = fields(14).toLong
        val HISTORY: String = fields(15)
        val ORIG_PKTS: Long = fields(16).toLong
        val ORIG_IP_BYTES: Long = fields(17).toLong
        val RESP_PKTS: Long = fields(18).toLong
        val RESP_IP_BYTES: Long = fields(19).toLong
        val TUNNEL_PARENT: String = fields(20)

        connLogEntry(TS, UID, SRCADDR, SRCPORT, DESTADDR, DESTPORT, PROTOCOL, SERVICE, DURATION, ORIG_BYTES, RESP_BYTES,
          CONN_STATE, LOCAL_ORIG, LOCAL_RESP, MISSED_BYTES, HISTORY, ORIG_PKTS, ORIG_IP_BYTES, RESP_PKTS, RESP_IP_BYTES,
          TUNNEL_PARENT)
      } catch {
        case _: Throwable => connLogEntry()
      }
    }.filter(_.TS != "")
  }

  private def mergeEntries(left: connLogEntry, right: connLogEntry): connLogEntry = {
    val v = if (left.DESC != "") {
      connLogEntry(
        right.TS,
        right.UID,
        right.SRCADDR,
        right.SRCPORT,
        right.DESTADDR,
        right.DESTPORT,
        right.PROTOCOL,
        right.SERVICE,
        right.DURATION,
        right.ORIG_BYTES,
        right.RESP_BYTES,
        right.CONN_STATE,
        right.LOCAL_ORIG,
        right.LOCAL_RESP,
        right.MISSED_BYTES,
        right.HISTORY,
        right.ORIG_PKTS,
        right.ORIG_IP_BYTES,
        right.RESP_PKTS,
        right.RESP_IP_BYTES,
        right.TUNNEL_PARENT,
        left.DESC
      )
    } else {
      connLogEntry(
        left.TS,
        left.UID,
        left.SRCADDR,
        left.SRCPORT,
        left.DESTADDR,
        left.DESTPORT,
        left.PROTOCOL,
        left.SERVICE,
        left.DURATION,
        left.ORIG_BYTES,
        left.RESP_BYTES,
        left.CONN_STATE,
        left.LOCAL_ORIG,
        left.LOCAL_RESP,
        left.MISSED_BYTES,
        left.HISTORY,
        left.ORIG_PKTS,
        left.ORIG_IP_BYTES,
        left.RESP_PKTS,
        left.RESP_IP_BYTES,
        left.TUNNEL_PARENT,
        right.DESC
      )
    }
    v
  }

  /***
    * This is a function that is called to find the ONE connection that is within the time stamp
    * @param TSofAlert the timestamp given by snort
    * @param connEntries the entries that match the src ip and port and dest ip and port
    * @param DESC the description snort gives of the attack
    * @return
    */
  private def reduceRecords(TSofAlert: Double, connEntries: Array[connLogEntry], DESC: String): connLogEntry =
  {
    val timeBuffer = 1.0
    for(x <- connEntries)
      {
//        println("here is alert time " + TSofAlert + " < " + (x.TS.toDouble + x.DURATION - timeBuffer) + " and " + TSofAlert + " > " + (x.TS.toDouble - timeBuffer))
        if(TSofAlert <= x.TS.toDouble + x.DURATION + timeBuffer && TSofAlert >= x.TS.toDouble - timeBuffer)
          {
            val temp = new connLogEntry(x.TS,x.UID, x.SRCADDR, x.SRCPORT, x.DESTADDR, x.DESTPORT, x.PROTOCOL, x.SERVICE, x.DURATION, x.ORIG_BYTES,
              x.RESP_BYTES, x.CONN_STATE, x.LOCAL_ORIG, x.LOCAL_RESP, x.MISSED_BYTES, x.HISTORY, x.ORIG_PKTS, x.ORIG_IP_BYTES, x.RESP_PKTS, x.RESP_IP_BYTES, x.TUNNEL_PARENT, DESC)
            return temp
          }
      }
    return null
  }

  /***
    * This method returns all the connections that were flagged as port scans.  It is important to note that this function
    * returns an array.  This is because snort only gives one alert for a port scan, but a port scan creates several connections
    * and we need to flag all those connections.
    * @param TSofAlert
    * @param connEntry
    * @param DESC
    * @return
    */
  private def reducePortScanRecords(TSofAlert: Double, connEntry: connLogEntry, DESC: String): connLogEntry =
  {
    val timeBuffer: Double = 5.0

    if(TSofAlert <= connEntry.TS.toDouble + timeBuffer && TSofAlert >= connEntry.TS.toDouble - timeBuffer )
    {
      val temp = new connLogEntry(connEntry.TS,connEntry.UID, connEntry.SRCADDR, connEntry.SRCPORT, connEntry.DESTADDR, connEntry.DESTPORT, connEntry.PROTOCOL, connEntry.SERVICE, connEntry.DURATION, connEntry.ORIG_BYTES,
        connEntry.RESP_BYTES, connEntry.CONN_STATE, connEntry.LOCAL_ORIG, connEntry.LOCAL_RESP, connEntry.MISSED_BYTES, connEntry.HISTORY, connEntry.ORIG_PKTS, connEntry.ORIG_IP_BYTES, connEntry.RESP_PKTS, connEntry.RESP_IP_BYTES, connEntry.TUNNEL_PARENT, DESC)
      return temp
    }
    return null
  }

  private def getAugLogInfo(snortEntries: RDD[alertBlock], broEntries: RDD[connLogEntry], augLog: String): Unit = {
    //println(snortEntries.count())

    broEntries.count()

    //first we set an rdd with the following characteristics
    //((SrcIP, SrcPort, destIP, destPort), connLogEntry) where connLogEntry is just a container of all the info in a conn line
    //keep in mind that a snort alert does not give as much info as a conn line but that is ok.  Using the srcIP+port, destIP+Port and Time stamp we can narrow down the connections in the conn file
    val sn2bro: RDD[((String, Int, String, Int), connLogEntry)] = snortEntries.map(alert =>  connLogEntry(TS = alert.timeStamp, SRCADDR = alert.srcIP, SRCPORT = alert.srcPort,
      DESTADDR = alert.dstIP, DESTPORT = alert.dstPort, DESC = alert.attackName)).map(entry => ((entry.SRCADDR, entry.SRCPORT, entry.DESTADDR, +entry.DESTPORT), entry))


    //next we get the actual bro entries from well bro:P
    val keyedBroEntries = broEntries.map(entry => ((entry.SRCADDR, entry.SRCPORT, entry.DESTADDR, entry.DESTPORT), entry))


    //next we group the two entries up.
    //heres the process, first we group each rdd up that has the same key,
    //next we get a list of keys with an array of potential values (has the same src and dest parameters)
    //clearly just matching the src and dest isn't enough...you need to include the time stamp also
    //after that we filter the result to get rid of snort entries that were not found

    //the next question you may ask is why is there a difference between port scans and not port scans?
    //the answer is simple.  snort does not give a src port or dest port for port scans.  If you think about it it makes sense:P


    val augEntriesWithoutPortscans = sn2bro.leftOuterJoin(keyedBroEntries)
      .filter(record => record._2._2.isDefined)
        .map(record => (record._1, reduceRecords(record._2._1.TS.toDouble / 1000, record._2._2.toArray, record._2._1.DESC)))
      .filter(record => record._2 != null)




    val augEntriesPortScans = sn2bro.map(entry => ((entry._1._1, entry._1._3), entry._2))
      .leftOuterJoin(keyedBroEntries.map(entry => ((entry._1._1, entry._1._3), entry._2)))
        .filter(record => record._2._2.isDefined)
        .flatMap(record => for(x <- record._2._2) yield (record._1, record._2._1, x))
      .map(record => (record._1, reducePortScanRecords(record._2.TS.toDouble / 1000, record._3, record._2.DESC)))
        .filter(record => record._2  != null)


    //this combines all the bad connections
    val totalBadEntries = augEntriesPortScans.map(record => record._2).union(augEntriesWithoutPortscans.map(record => record._2))

    //this will replace the conn entries that were flagged as bad with the version that includes the description of the attack
    val allEntries = broEntries.union(totalBadEntries).map(record => (record.TS, record))
      .reduceByKey((record1, record2) =>  if(record1.DESC != "") record1 else record2)
      .map(record => record._2).sortBy(entry => entry.TS.toDouble) //may get rid of sort



    try {

      val augOut = Array(
        "#separator \\x09",
        "#set_separator  ,",
        "#empty_field    (empty)",
        "#unset_field    -",
        "#path   conn",
        "#open   2016-09-15-15-59-01",
        "#fields ts      uid     id.orig_h       id.orig_p       id.resp_h       id.resp_p       proto   service duration        orig_bytes      resp_bytes      conn_state      local_orig      local_resp      missed_bytes    history orig_pkts      orig_ip_bytes    resp_pkts       resp_ip_bytes   tunnel_parents Desc",
        "#types  time    string  addr    port    addr    port    enum    string  intervalcount   count   string  bool    bool    count   string  count   count   count  count    set[string]  string"
      ) ++ allEntries.map(entry =>
        entry.TS + "\t" +
          entry.UID + "\t" +
          entry.SRCADDR + "\t" +
          entry.SRCPORT + "\t" +
          entry.DESTADDR + "\t" +
          entry.DESTPORT + "\t" +
          entry.PROTOCOL + "\t" +
          entry.SERVICE + "\t" +
          entry.DURATION.toString + "\t" +
          entry.ORIG_BYTES.toString + "\t" +
          entry.RESP_BYTES.toString + "\t" +
          entry.CONN_STATE + "\t" +
          entry.LOCAL_ORIG + "\t" +
          entry.LOCAL_RESP + "\t" +
          entry.MISSED_BYTES.toString + "\t" +
          entry.HISTORY + "\t" +
          entry.ORIG_PKTS.toString + "\t" +
          entry.ORIG_IP_BYTES.toString + "\t" +
          entry.RESP_PKTS.toString + "\t" +
          entry.RESP_IP_BYTES.toString + "\t" +
          entry.TUNNEL_PARENT + "\t" +
          entry.DESC).collect()

      val file = new File(augLog)
      val bw = new BufferedWriter(new FileWriter(file))
      for (entry <- augOut) {
        bw.write(entry + "\n")
      }
      bw.close()

    } catch {
      case _: Throwable => return
    }
  }


  private def mv(oldName: String, newName: String) =
    try {
      new File(oldName).renameTo(new File(newName));
      true
    }
    catch {
      case _: Throwable => false
    }

  def run(alertLog: String, connLog: String, augLog: String): Unit = {
    val snortEntries = getSnortAlertInfo(alertLog).cache()
    val broEntries = getBroLogInfo(connLog).cache()

    getAugLogInfo(snortEntries, broEntries, augLog)

  }
}
