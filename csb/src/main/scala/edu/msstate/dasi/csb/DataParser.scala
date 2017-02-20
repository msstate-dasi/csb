package edu.msstate.dasi.csb

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

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


  def tempReadFromConn(sc: SparkContext, partitions: Int, connFile: String): Graph[VertexData, EdgeData] = {

    val combined = sc.textFile(connFile).filter(line => !line.contains("#")).collect.map { line =>
      try {
        val splits = line.split('\t')
        (splits(0).toLong - 1, splits(1).toLong - 1)
      } catch {
        case _: Throwable =>
          (-1L, -1L)
      }
    }.filter(record => record._1 != -1)

    val vertices = combined.flatMap(record => Array(record._1, record._2)).distinct.map(record => (record, VertexData()))
    val verticesHash = new mutable.HashMap[Long,Long]()

    var idx = 0L
    for (v <- vertices) {
      verticesHash.put(v._1, idx)
      idx += 1L
    }

    val edges = combined.map(record => (verticesHash(record._1), verticesHash(record._2))).map(record => new Edge[EdgeData](record._1, record._2, EdgeData()))

    var hash = new mutable.HashMap[Int, Int]()

    val nodeCount = verticesHash.size
    println("nodeList " + nodeCount + "edge count " + edges.length)
    val thing = Graph[VertexData, EdgeData](sc.parallelize(vertices), sc.parallelize(edges))
    return thing
  }
  def logToGraph(augLogPath: String, partitions: Int): Graph[VertexData, EdgeData] = {
    val augLogFile = sc.textFile(augLogPath, partitions)

    // Drop the 8-lines header and filter lines that contains only IPv4 addresses
    val augLog = augLogFile.mapPartitionsWithIndex { (idx, lines) => if (idx == 0) lines.drop(8) else lines }
      .filter(isInet4).filter(isAllowedProto)

    val edges = augLog.map(line => {
      val pieces = line.split('\t')

      val origIp = pieces(2)
      val origPort = pieces(3)
      val srcId = inetToLong(origIp) | (origPort.toLong << 32)

      val respIp = pieces(4)
      val respPort = pieces(5)
      val dstId = inetToLong(respIp) | (respPort.toLong << 32)

      Edge(srcId, dstId, EdgeData(
        /* ts = new Date(pieces(0).split('.')(0).toLong * 1000), */
        /* uid = pieces(1), */
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
        respIpBytes = pieces(19).toLong,
        /* tunnelParents = pieces(20), */
        // TODO: why is the following check needed?
        desc = if (pieces.length > 21) pieces(21) else "")
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
