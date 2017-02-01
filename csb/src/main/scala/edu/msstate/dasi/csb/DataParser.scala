package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object DataParser {
  def readFromConnFile(partitions: Int, connFile: String): Graph[VertexData, EdgeData] = {
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

    Graph(
      vertices,
      Edges,
      null.asInstanceOf[VertexData],
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK
    )
  }
}
