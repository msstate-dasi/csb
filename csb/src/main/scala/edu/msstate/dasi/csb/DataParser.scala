package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel

object DataParser {
  private def isAllowedProto(line: String): Boolean = {
    val pieces = line.split('\t')
    pieces(6).contains("tcp") || pieces(6).contains("udp")
  }

  private def isInet4(line: String): Boolean = {
    val pieces = line.split('\t')
    ! ( pieces(2).contains(':') || pieces(4).contains(':') )
  }

  private def inetToLong(inet: String): Long = {
    val pieces = inet.split('.')
    pieces.zipWithIndex.map { case (value, index) => value.toLong << 8 * ((pieces.length - 1) - index) }.sum
  }


  def logToGraph(logPath: String, partitions: Int): Graph[VertexData, EdgeData] = {
    val logFile = sc.textFile(logPath, partitions)

    // Drop the 8-lines header and filter lines that contains only IPv4 addresses
    val theLog = logFile.mapPartitionsWithIndex { (idx, lines) => if (idx == 0) lines.drop(8) else lines }
      .filter(isInet4).filter(isAllowedProto)

    val edges = theLog.map(line => {
      val pieces = line.split('\t')

      val origIp = pieces(2)
      val srcId = inetToLong(origIp)

      val respIp = pieces(4)
      val dstId = inetToLong(respIp)

      Edge(srcId, dstId, EdgeData(
        ts = pieces(0).split('.')(0).toLong,
        /* uid = pieces(1), */
        origPort = pieces(3).toInt,
        respPort = pieces(5).toInt,
        proto = pieces(6),
        /* service = pieces(7), */
        duration = pieces(8).toDouble,
        origBytes = pieces(9).toLong,
        respBytes = pieces(10).toLong,
        connState = pieces(11),
        /* localOrig = pieces(12).toBoolean, */
        /* localResp = pieces(13).toBoolean, */
        /* missedBytes = pieces(14).toLong, */
        /* history = pieces(15), */
        origPkts = pieces(16).toLong,
        origIpBytes = pieces(17).toLong,
        respPkts = pieces(18).toLong,
        respIpBytes = pieces(19).toLong
        /* tunnelParents = pieces(20), */
      )
      )
    })

    Graph.fromEdges(
      edges,
      null.asInstanceOf[VertexData],
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    )
  }
}
