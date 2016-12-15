package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

trait data_Parser extends java.io.Serializable {

  def readFromConnFile(sc: SparkContext, connFile: String): (RDD[(VertexId,nodeData)], RDD[Edge[edgeData]]) = {
    //If we are opening a conn.log file
    val file = sc.textFile(connFile)

    //I get a list of all the lines of the conn.log file in a way for easy parsing
    val lines = file.map(line => line.split("\n")).filter(line => !line(0).contains("#")).map(line => line(0).replaceAll("-","0"))

    //Next I get each line in a list of the edge that line in conn.log represents and the vertices that make that edge up
    //NOTE: There will be many copies of the vertices which will be reduced later
    var connPLUSnodes = lines.map(line => (edgeData(
      line.split("\t")(0),
      line.split("\t")(6),
      line.split("\t")(9).toDouble,
      line.split("\t")(9).toLong,
      line.split("\t")(10).toLong,
      line.split("\t")(11),
      line.split("\t")(12).toLong,
      line.split("\t")(17).toLong,
      line.split("\t")(18).toLong,
      line.split("\t")(19).toLong,
      ""),
      nodeData(line.split("\t")(2) + ":" + line.split("\t")(3)),
      nodeData(line.split("\t")(4) + ":" + line.split("\t")(5))))


    //from connPLUSnodes lets grab all the DISTINCT nodes
    var ALLNODES : RDD[nodeData] = connPLUSnodes.map(record => record._2).union(connPLUSnodes.map(record => record._3)).distinct()

    //next lets give them numbers and let that number be the "key"(basically index for my use)
    var vertices: RDD[(VertexId, nodeData)] = ALLNODES.zipWithIndex().map(record => (record._2, record._1))


    //next I make a hashtable of the nodes with it's given index.
    //I have to do this since RDD transformations cannot happen within
    //other RDD's and hashtables have O(1)
    var verticesList = ALLNODES.collect()
    var hashTable = new scala.collection.mutable.HashMap[nodeData, VertexId]
    for( x<-0 to verticesList.length - 1)
    {
      hashTable.put(verticesList(x), x.toLong)
    }



    //Next I generate the edge list with the vertices represented by indexes(as it wants it)
    var Edges: RDD[Edge[edgeData]] = connPLUSnodes.map(record => Edge[edgeData](hashTable.get(record._2).head, hashTable.get(record._3).head, record._1))


    return (vertices, Edges)
  }

  def readFromSeedGraph(sc: SparkContext, seedVertFile: String,seedEdgeFile: String): (RDD[(VertexId,nodeData)], RDD[Edge[edgeData]]) = {

    val inVertices: RDD[(VertexId,nodeData)] = sc.textFile(seedVertFile).map(line => line.stripPrefix("(").stripSuffix(")").split(',')).map { record =>
      parseNodeData(record)
    }.filter(_._1 != false).map(record => record._2)

    val inEdges: RDD[Edge[edgeData]] = sc.textFile(seedEdgeFile).map(line => line.stripPrefix("Edge(").stripSuffix(")").split(",", 3)).map { record =>
      parseEdgeData(record)
    }.filter(_._1 != false).map(record => record._2)

    return (inVertices,inEdges)
  }

  /*** Parses Data for an edge out of an array of strings
    *
    * @param inData Array of strings to parse as edge data. First element is source ID, second element is destination ID, third element is the edge data
    * @return (bool whether the record was successfully parsed, record(VertexID, VertexID, edu.msstate.dasi.edgeData))
    */
  private def parseEdgeData(inData: Array[String]): (Boolean, Edge[edgeData]) = {
    val result: (Boolean, Edge[edgeData]) =
      try {
        val srcNode = inData(0).toLong
        val dstNode = inData(1).toLong

        //Just a bunch of string formatting and splitting
        val edgeStrs = inData(2).stripPrefix("edgeData(").stripSuffix(")").split(',')
        //println(inData(2).stripPrefix("edgeData(").stripSuffix(")"))
        val dP = edgeData()
        val TS: String = try { edgeStrs(0) } catch { case _: Throwable => dP.TS}
        //println("TS: \"" + TS + "\"")
        val PROTOCOL: String =try { edgeStrs(1) } catch { case _: Throwable => dP.PROTOCOL}
        //println("PROTOCOL: \"" + PROTOCOL + "\"")
        val DURATION: Double = try { edgeStrs(2).toDouble } catch { case _: Throwable => dP.DURATION}
        //println("DURATION: \"" + DURATION + "\"")
        val ORIG_BYTES: Long = try { edgeStrs(3).toLong } catch { case _: Throwable => dP.ORIG_BYTES}
        //println("ORIG_BYTES: \"" + ORIG_BYTES + "\"")
        val RESP_BYTES: Long = try { edgeStrs(4).toLong } catch { case _: Throwable => dP.RESP_BYTES}
        //println("RESP_BYTES: \"" + RESP_BYTES + "\"")
        val CONN_STATE: String = try { edgeStrs(5) } catch { case _: Throwable => dP.CONN_STATE}
        //println("CONN_STATE: \"" + CONN_STATE + "\"")
        val ORIG_PKTS: Long = try { edgeStrs(6).toLong } catch { case _: Throwable => dP.ORIG_PKTS}
        //println("ORIG_PKTS: \"" + ORIG_PKTS + "\"")
        val ORIG_IP_BYTES: Long = try { edgeStrs(7).toLong } catch { case _: Throwable => dP.ORIG_IP_BYTES}
        //println("ORIG_IP_BYTES: \"" + ORIG_IP_BYTES + "\"")
        val RESP_PKTS: Long = try { edgeStrs(8).toLong } catch { case _: Throwable => dP.RESP_PKTS}
        //println("RESP_PKTS: \"" + RESP_PKTS + "\"")
        val RESP_IP_BYTES: Long = try { edgeStrs(9).toLong } catch { case _: Throwable => dP.RESP_IP_BYTES}
        //println("RESP_IP_BYTES: \"" + RESP_IP_BYTES + "\"")

        val DESC: String = if (edgeStrs.length > 9) edgeStrs(0) else ""

        //println("DESC: \"" + DESC + "\"")
        //println()

        (true, Edge(srcNode, dstNode, edgeData(TS, PROTOCOL, DURATION, ORIG_BYTES, RESP_BYTES, CONN_STATE, ORIG_PKTS, ORIG_IP_BYTES, RESP_PKTS, RESP_IP_BYTES, DESC)))
      } catch {
        case _: Throwable =>
          println("!!! THERE MAY BE ERRORS IN THE DATASET !!!")
          (false, Edge(0L, 0L, edgeData("", "", 0, 0, 0, "", 0, 0, 0, 0, "")))
      }

    //return
    result
  }


  /*** Parses data for a node out of an array of strings
    *
    * @param inData Array strings to parse as node data. First element is the ID of the node, second element is the description of the node
    * @return Tuple (bool whether the record was successfully parsed, record(VertexID, edu.msstate.dasi.nodeData))
    */
  private def parseNodeData(inData: Array[String]): (Boolean, (Long, nodeData))  = {
    val result: (Boolean, (VertexId, nodeData)) =
      try {
        (true, (inData(0).toLong, nodeData(inData(1).stripPrefix("nodeData(").stripSuffix(")"))))
      } catch {
        case _: Throwable =>
          println("!!! THERE MAY BE ERRORS IN THE DATASET !!!")
          (false, (0L, nodeData("")))
      }

    result
  }
}