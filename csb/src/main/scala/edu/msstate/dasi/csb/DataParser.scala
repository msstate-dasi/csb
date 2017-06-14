package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.model.{ConnStates, EdgeData, Protocols, VertexData}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel

/**
 * Handles log data parsing for the application.
 */
object DataParser {

  /**
   * Checks if a connection protocol is either TCP or UDP, otherwise drops the connection.
   *
   * @param line Single line of Bro IDS log to check.
   *
   * @return True if the connection is either TCP or UDP, otherwise false
   */
  private def isAllowedProto(line: String): Boolean = {
    val pieces = line.split('\t')
    pieces(6).contains("tcp") || pieces(6).contains("udp")
  }

  /**
   * Checks if both addresses conform to IPv4, otherwise drops the connection
   *
   * @param line Single line of Bro IDS log to check.
   *
   * @return True if both addresses are IPv4, false if otherwise.
   */
  private def isInet4(line: String): Boolean = {
    val pieces = line.split('\t')
    !(pieces(2).contains(':') || pieces(4).contains(':'))
  }

  /**
   * Converts an IPv4 address into a Long integer.
   *
   * @param inet String containing IPv4 address separated by periods.
   *
   * @return Long integer containing the numerical IPv4
   */
  private def inetToLong(inet: String): Long = {
    val pieces = inet.split('.')
    pieces.zipWithIndex.map { case (value, index) => value.toLong << 8 * ((pieces.length - 1) - index) }.sum
  }

  /**
   * Method to parse a Bro IDS log and convert it into a seed graph object.
   *
   * @param logPath    File path to a Bro IDS Connection log
   * @param partitions Number of partitions to use when parsing the log file as a Spark text object.
   *
   * @return Graph containing the filtered log entries.
   */
  def logToGraph(logPath: String, partitions: Int): Graph[VertexData, EdgeData] = {
    val logFile = sc.textFile(logPath, partitions)

    // Drop the 8-lines header and 1-line footer and filter lines that contains only IPv4 addresses
    val theLog = logFile.filter(line => line(0) != '#').filter(isInet4).filter(isAllowedProto)

    val edges = theLog.map(line => {
      val pieces = line.split('\t')

      val origIp = pieces(2)
      val srcId = inetToLong(origIp)

      val respIp = pieces(4)
      val dstId = inetToLong(respIp)

      Edge(srcId, dstId, EdgeData(
        ts = try { pieces(0).split('.')(0).toLong } catch { case _: NumberFormatException => 0L},
        /* uid = pieces(1), */
        origPort = try {pieces(3).toInt } catch { case _: NumberFormatException => 0},
        respPort = try {pieces(5).toInt } catch { case _: NumberFormatException => 0},
        proto = Protocols.withName(pieces(6).toUpperCase),
        /* service = pieces(7), */
        duration = try {pieces(8).toDouble } catch { case _: NumberFormatException => 0.0},
        origBytes = try {pieces(9).toLong } catch { case _: NumberFormatException => 0L},
        respBytes = try {pieces(10).toLong } catch { case _: NumberFormatException => 0L},
        connState = ConnStates.withName(pieces(11).toUpperCase),
        /* localOrig = pieces(12).toBoolean, */
        /* localResp = pieces(13).toBoolean, */
        /* missedBytes = pieces(14).toLong, */
        /* history = pieces(15), */
        origPkts = try {pieces(16).toLong } catch { case _: NumberFormatException => 0L},
        origIpBytes = try {pieces(17).toLong } catch { case _: NumberFormatException => 0L},
        respPkts = try {pieces(18).toLong } catch { case _: NumberFormatException => 0L},
        respIpBytes = try {pieces(19).toLong } catch { case _: NumberFormatException => 0L}
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
