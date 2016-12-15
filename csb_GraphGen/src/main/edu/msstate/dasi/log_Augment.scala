package edu.msstate.dasi

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by B1nary on 12/8/2016.
  */
case class alertBlock (
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

class log_Augment {

  def getSnortAlertInfo(sc: SparkContext, alertLog: String): RDD[alertBlock] = {
    val pattern = "%m/%d/%Y-%H:%M:%S"
    val dateFormatter = new SimpleDateFormat(pattern)

    val result = sc.wholeTextFiles(alertLog).flatMap(x => x._2.split("\n\n")).map { block =>
      try {
        val blockList = block.split("\n")
        val attackName = blockList(0) + "\n" + blockList(1)

        val srcIP = blockList(2).split(" ")(1).split(":")(0)
        var srcPort = 0
        if (blockList(2).split(" ")(1).split(":").length > 1)
          srcPort = blockList(2).split(" ")(1).split(":")(1).toInt

        val dstIP = blockList(2).split(" ")(3).split(":")(0)
        var dstPort = 0
        if (blockList(2).split(" ")(3).split(":").length > 1)
          dstPort = blockList(2).split(" ")(3).split(":")(1).toInt

        val dateTime = dateFormatter.parse(blockList(2).split(" ")(0))
        val timeStamp = dateTime.getTime.toString

        alertBlock(attackName, srcIP, srcPort, dstIP, dstPort, timeStamp)
      } catch {case _ => alertBlock()}
    }.filter(_.timeStamp != "")

    result
  }

  def getBroLogInfo(sc: SparkContext, connLog: String): RDD[connLogEntry] = {
    sc.textFile(connLog).map(line =>
      try {
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
        val lOCAL_ORIG: String = fields(13)
        val LOCAL_RESP: String = fields(14)
        val MISSED_BYTES: Long = fields(15).toLong
        val HISTORY: String = fields(16)
        val ORIG_PKTS: Long = fields(17).toLong
        val ORIG_IP_BYTES: Long = fields(18).toLong
        val RESP_PKTS: Long = fields(19).toLong
        val RESP_IP_BYTES: Long = fields(20).toLong
        val TUNNEL_PARENT: String = fields(21)

        connLogEntry(TS, UID, SRCADDR, SRCPORT, DESTADDR, DESTPORT, PROTOCOL, SERVICE, DURATION, ORIG_BYTES, RESP_BYTES,
          CONN_STATE, LOCAL_ORIG, LOCAL_RESP, MISSED_BYTES, HISTORY, ORIG_PKTS, ORIG_IP_BYTES, RESP_PKTS, RESP_IP_BYTES,
          TUNNEL_PARENT)
      } catch { case _ => connLogEntry() }
    ).filter(_.TS != "")
  }

  def getAugLogInfo(sc: SparkContext, snortEntries: RDD[alertBlock], broEntries: RDD[connLogEntry], augLog: String): RDD[connLogEntry] = {
    val sn2bro = snortEntries.map(alert => connLogEntry(TS = alert.timeStamp, SRCADDR = alert.srcIP, SRCPORT=alert.srcPort,
      DESTADDR = alert.dstIP, DESTPORT = alert.dstPort, DESC = alert.attackName))
      .map(entry => (entry.SRCADDR + entry.SRCPORT + entry.DESTADDR + entry.DESTPORT, entry)).collect()

    val keyedBroEntries = broEntries.map(entry => (entry.SRCADDR+entry.SRCPORT+entry.DESTADDR+entry.DESTPORT, entry))

    /*
    sn2bro.join(keyedBroEntries).map(p => connLogEntry(p._2._1.TS, p._2._1.UID, p._2._1.SRCADDR, p._2._1.SRCPORT,
      p._2._1.DESTADDR, p._2._1.DESTPORT, p._2._1.PROTOCOL, p._2._1.SERVICE, p._2._1.DURATION, p._2._1.ORIG_BYTES,
      p._2._1.RESP_BYTES, p._2._1.CONN_STATE, p._2._1.LOCAL_ORIG, p._2._1.LOCAL_RESP, p._2._1.MISSED_BYTES,
      p._2._1.HISTORY, p._2._1.ORIG_PKTS, p._2._1.ORIG_IP_BYTES, p._2._1.RESP_PKTS, p._2._1.RESP_IP_BYTES,
      p._2._1.TUNNEL_PARENT))
    */

    var augEntries: Array[connLogEntry] = Array.empty[connLogEntry]

    for(entry <- sn2bro) {
      var matches = keyedBroEntries.filter(x => x._1.matches(entry._1))

      var results: Array[connLogEntry] = null

      if (matches.isEmpty) {
        matches = keyedBroEntries.filter(x => x._1.matches(".*" +  entry._2.SRCADDR + ".*" + entry._2.DESTADDR + ".*") ||
                                              x._1.matches(".*" +  entry._2.DESTADDR + ".*" + entry._2.SRCADDR + ".*"))
      }

      if(!matches.isEmpty()) {
        for(m <- matches) {
          if (entry._2.TS.toDouble >= m._2.TS.toDouble - 5 &&
            entry._2.TS.toDouble <= m._2.TS.toDouble + m._2.DURATION.toDouble + 5) {
            results = results :+ m._2
          }
        }
        
        if (results != null) {
          for(result <- results) {
            augEntries = augEntries :+ connLogEntry(result.TS, result.UID, result.SRCADDR, result.SRCPORT,
              result.DESTADDR, result.DESTPORT, result.PROTOCOL, result.SERVICE, result.DURATION, result.ORIG_BYTES,
              result.RESP_BYTES, result.CONN_STATE, result.LOCAL_ORIG, result.LOCAL_RESP, result.MISSED_BYTES,
              result.HISTORY, result.ORIG_PKTS, result.ORIG_IP_BYTES, result.RESP_PKTS, result.RESP_IP_BYTES,
              result.TUNNEL_PARENT, entry._2.DESC)
          }
        }
      }
    }

    val augRDD = sc.parallelize(augEntries)

    try {
      augRDD.repartition(1).saveAsTextFile(augLog)
      val sparkOutput = new File(augLog+"\\part-00000").toPath()
      Files.move(sparkOutput, new File(augLog).toPath(), StandardCopyOption.ATOMIC_MOVE)
    }

    augRDD
  }

  def mv(oldName: String, newName: String) =
    try { new File(oldName).renameTo(new File(newName)); true } catch {case _: Throwable => false}

  def run(sc: SparkContext, alertLog: String, connLog: String, augLog: String): Unit = {
    val snortEntries = getSnortAlertInfo(sc, alertLog)
    val broEntries = getBroLogInfo(sc, connLog)

    val augLogEntries = getAugLogInfo(sc: SparkContext, snortEntries, broEntries, augLog)

    augLogEntries.repartition(1).saveAsTextFile(augLog)

    mv(augLog+"\\part-00000", augLog)
  }
}
