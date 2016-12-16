package edu.msstate.dasi

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, StandardCopyOption}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by B1nary on 12/8/2016.
  */
case class alertBlock(
                       attackName: String = "",
                       srcIP: String = "",
                       srcPort: Int = 0,
                       dstIP: String = "",
                       dstPort: Int = 0,
                       timeStamp: String = ""
                     )

case class connLogEntry(TS: String = "",
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

class log_Augment extends Serializable {


  def getDate(in: Array[String]): String = {
    //in.foreach(println)
    val pattern = "MM/dd/yy-HH:mm:ss"
    val dateFormatter = new SimpleDateFormat(pattern)
    val dateTime = dateFormatter.parse(in(2).split(" ")(0).split('.').head)
    dateTime.getTime.toString
  }

  def getSnortAlertInfo(sc: SparkContext, alertLog: String): RDD[alertBlock] = {
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

  def getBroLogInfo(sc: SparkContext, connLog: String): RDD[connLogEntry] = {
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

  def mergeEntries(left: connLogEntry, right: connLogEntry): connLogEntry = {
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

  def getAugLogInfo(sc: SparkContext, snortEntries: RDD[alertBlock], broEntries: RDD[connLogEntry], augLog: String): Unit = {
    //println(snortEntries.count())

    broEntries.count()

    val sn2bro: RDD[((String, Int, String, Int), connLogEntry)] = snortEntries.map(alert => connLogEntry(TS = alert.timeStamp, SRCADDR = alert.srcIP, SRCPORT = alert.srcPort,
      DESTADDR = alert.dstIP, DESTPORT = alert.dstPort, DESC = alert.attackName)).map(entry => ((entry.SRCADDR, entry.SRCPORT, entry.DESTADDR, +entry.DESTPORT), entry))

    val keyedBroEntries = broEntries.map(entry => ((entry.SRCADDR, entry.SRCPORT, entry.DESTADDR, entry.DESTPORT), entry))

    /*
    sn2bro.join(keyedBroEntries).map(p => connLogEntry(p._2._1.TS, p._2._1.UID, p._2._1.SRCADDR, p._2._1.SRCPORT,
      p._2._1.DESTADDR, p._2._1.DESTPORT, p._2._1.PROTOCOL, p._2._1.SERVICE, p._2._1.DURATION, p._2._1.ORIG_BYTES,
      p._2._1.RESP_BYTES, p._2._1.CONN_STATE, p._2._1.LOCAL_ORIG, p._2._1.LOCAL_RESP, p._2._1.MISSED_BYTES,
      p._2._1.HISTORY, p._2._1.ORIG_PKTS, p._2._1.ORIG_IP_BYTES, p._2._1.RESP_PKTS, p._2._1.RESP_IP_BYTES,
      p._2._1.TUNNEL_PARENT))
    */

    val augEntriesWithoutPortscans = sn2bro.leftOuterJoin(keyedBroEntries)
      .filter(record => record._2._2.isDefined)
      .flatMap(record => Array((record._1, record._2._1)) ++ (for (entry <- record._2._2.toSeq) yield (record._1, entry)))
      .reduceByKey(mergeEntries)

    val augEntriesPortScans = sn2bro.map(entry => ((entry._1._1, entry._1._3), entry._2))
      .leftOuterJoin(keyedBroEntries.map(entry => ((entry._1._1, entry._1._3), entry._2)))
      .filter(record => record._2._2.isDefined)
      .flatMap(record => Array((record._1, record._2._1)) ++ (for (entry <- record._2._2.toSeq) yield (record._1, entry)))
      .reduceByKey(mergeEntries)

    val augEntries = augEntriesWithoutPortscans.map(entry => ((entry._1._1, entry._1._3), entry._2)).union(augEntriesPortScans).reduceByKey(mergeEntries).map(entry => entry._2)
    val totalEntries = broEntries.map(entry => (entry.TS, entry)).union(augEntries.map(entry => (entry.TS, entry))).reduceByKey((left, right) => right).map(entry => entry._2)

    try {

      val augOut = Array(
        "#separator \\x09",
        "#set_separator  ,",
        "#empty_field    (empty)",
        "#unset_field    -",
        "#path   conn",
        "#open   2016-09-15-15-59-01",
        "#fields ts      uid     id.orig_h       id.orig_p       id.resp_h       id.resp_p       proto   service duration        orig_bytes      resp_bytes      conn_state      local_orig      local_resp      missed_bytes    history orig_pkts      orig_ip_bytes    resp_pkts       resp_ip_bytes   tunnel_parents",
        "#types  time    string  addr    port    addr    port    enum    string  intervalcount   count   string  bool    bool    count   string  count   count   count  count    set[string]"
      ) ++ totalEntries.map(entry =>
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


  def mv(oldName: String, newName: String) =
    try {
      new File(oldName).renameTo(new File(newName));
      true
    } catch {
      case _: Throwable => false
    }

  def run(sc: SparkContext, alertLog: String, connLog: String, augLog: String): Unit = {
    val snortEntries = getSnortAlertInfo(sc, alertLog).cache()
    val broEntries = getBroLogInfo(sc, connLog).cache()

    getAugLogInfo(sc: SparkContext, snortEntries, broEntries, augLog)

  }
}