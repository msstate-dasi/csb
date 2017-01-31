package edu.msstate.dasi.csb

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

trait DataParser {
  def readFromConnFile(sc: SparkContext, partitions: Int, connFile: String): (RDD[(VertexId, VertexData)], RDD[Edge[EdgeData]]) = {
    //If we are opening a conn.log file
    val file = sc.textFile(connFile, partitions)

    //I get a list of all the lines of the conn.log file in a way for easy parsing
    val lines = file.map(line => line.split("\n")).filter(line => !line(0).contains("#")).map(line => line(0).replaceAll("-","0"))

    //Next I get each line in a list of the edge that line in conn.log represents and the vertices that make that edge up
    //NOTE: There will be many copies of the vertices which will be reduced later
    val connPLUSnodes = lines.map(line => (EdgeData(
      line.split("\t")(0),
      line.split("\t")(6),
      line.split("\t")(8).toDouble,
      line.split("\t")(9).toLong,
      line.split("\t")(10).toLong,
      line.split("\t")(11),
      line.split("\t")(16).toLong,
      line.split("\t")(17).toLong,
      line.split("\t")(18).toLong,
      line.split("\t")(19).toLong,
      try { line.split("\t")(21) } catch {case _: Throwable => ""}
      ),
      VertexData(line.split("\t")(2) + ":" + line.split("\t")(3)),
      VertexData(line.split("\t")(4) + ":" + line.split("\t")(5))))

    //from connPLUSnodes lets grab all the DISTINCT nodes
    val ALLNODES : RDD[VertexData] = connPLUSnodes.map(record => record._2).union(connPLUSnodes.map(record => record._3)).distinct()

    //next lets give them numbers and let that number be the "key"(basically index for my use)
    val vertices: RDD[(VertexId, VertexData)] = ALLNODES.zipWithIndex().map(record => (record._2, record._1))

    //next I make a hashtable of the nodes with it's given index.
    //I have to do this since RDD transformations cannot happen within
    //other RDD's and hashtables have O(1)
    val verticesList = ALLNODES.collect()
    val hashTable = new scala.collection.mutable.HashMap[VertexData, VertexId]
    for( x <- verticesList.indices )
    {
      hashTable.put(verticesList(x), x.toLong)
    }

    //Next I generate the edge list with the vertices represented by indexes(as it wants it)
    val Edges: RDD[Edge[EdgeData]] = connPLUSnodes.map(record => Edge[EdgeData](hashTable.get(record._2).head, hashTable.get(record._3).head, record._1))

    (vertices, Edges)
  }

  def readFromSeedGraph(sc: SparkContext, partitions: Int, seedVertFile: String,seedEdgeFile: String): (RDD[(VertexId, VertexData)], RDD[Edge[EdgeData]]) = {

    val inVertices: RDD[(VertexId,VertexData)] = sc.textFile(seedVertFile, partitions).map(line => line.stripPrefix("(").stripSuffix(")").split(',')).map { record =>
      val inData = record

      try {
        (true, (inData(0).toLong, VertexData(inData(1).stripPrefix("VertexData(").stripSuffix(")"))))
      } catch {
        case _: Throwable =>
          println("!!! THERE MAY BE ERRORS IN THE DATASET !!!")
          (false, (0L, VertexData()))
      }
    }.filter(_._1 != false).map(record => record._2)

    val inEdges: RDD[Edge[EdgeData]] = sc.textFile(seedEdgeFile, partitions).map(line => line.stripPrefix("Edge(").stripSuffix(")").split(",", 3)).map { record =>
      val inData = record

      try {
        val srcNode = inData(0).toLong
        val dstNode = inData(1).toLong

        //Just a bunch of string formatting and splitting
        val edgeStrs = inData(2).stripPrefix("EdgeData(").stripSuffix(")").split(',')
        //println(inData(2).stripPrefix("EdgeData(").stripSuffix(")"))
        val dP = EdgeData()
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

        (true, Edge(srcNode, dstNode, EdgeData(TS, PROTOCOL, DURATION, ORIG_BYTES, RESP_BYTES, CONN_STATE, ORIG_PKTS, ORIG_IP_BYTES, RESP_PKTS, RESP_IP_BYTES, DESC)))
      } catch {
        case _: Throwable =>
          println("!!! THERE MAY BE ERRORS IN THE DATASET !!!")
          println()
          (false, Edge(0L, 0L, EdgeData()))
      }
    }.filter(_._1 != false).map(record => record._2)

    (inVertices,inEdges)
  }
}
