package edu.msstate.dasi.csb

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object Workload {
  /**
   * The number of vertices in the graph.
   */
  def countVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Long = graph.numVertices

  /**
   * The number of edges in the graph.
   */
  def countEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Long = graph.numEdges

  /**
   * The degree of each vertex in the graph.
   */
  def degree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Int] = graph.degrees

  /**
   * The in-degree of each vertex in the graph.
   */
  def inDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Int] = graph.inDegrees

  /**
   * The out-degree of each vertex in the graph.
   */
  def outDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Int] = graph.outDegrees

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   */
  def pageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] = {
    graph.pageRank(tol, resetProb)
  }

  //WORKLOAD ITEM 2
  /**
    * Collects list of neighbors based solely on incoming direction, and returns a list of
    * those neighbors as well as their node attribute
    * @param graph The input graph
    * @tparam VD Node attribute type for input graph
    * @tparam ED Edge attribute type for input graph
    * @return RDD of Arrays which contain VertexId and VD for each neighbor
    */
  def inNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]):
    VertexRDD[Array[(VertexId, VD)]] = graph.collectNeighbors(EdgeDirection.In)

  /**
    * Collects list of neighbors based solely on outgoing direction, and returns a list of
    * those neighbors as well as their node attribute
    * @param graph The input graph
    * @tparam VD Node attribute type for input graph
    * @tparam ED Edge attribute type for input graph
    * @return RDD of Arrays which contain VertexId and VD for each neighbor
    */
  def outNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]):
    VertexRDD[Array[(VertexId, VD)]] = graph.collectNeighbors(EdgeDirection.Out)

  /**
    * Collects list of neighbors in both incoming and outgoing direction, and returns a list of
    * those neighbors as well as their node attribute
    * @param graph The input graph
    * @tparam VD Node attribute type for input graph
    * @tparam ED Edge attribute type for input graph
    * @return RDD of Arrays which contain VertexId and VD for each neighbor
    */
  def neighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]):
    VertexRDD[Array[(VertexId, VD)]] = graph.collectNeighbors(EdgeDirection.Both)

  //WORKLOAD ITEM 4
  /**
    * Grabs all of the edges entering a node by grouping the edges by dstId attribute
    * @param graph The input graph
    * @tparam VD Node attribute type for input graph
    * @tparam ED Edge attribute type for input graph
    * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
    */
  def edgesEntering[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]):
    RDD[(VertexId, Iterable[Edge[ED]])] = graph.edges.groupBy(record => record.dstId)

  /**
    * Grabs all of the edges exiting a node by grouping the edges by srcId attribute
    * @param graph The input graph
    * @tparam VD Node attribute type for input graph
    * @tparam ED Edge attribute type for input graph
    * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
    */
  def edgesExiting[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]):
    RDD[(VertexId, Iterable[Edge[ED]])] = graph.edges.groupBy(record => record.srcId)

  //WORKLOAD ITEM 6
  /**
    * see below
    */
  def connectedComponents(): Unit = {
    // Not sure what this algorithm is supposed to do, because GraphX already has
    // a version of connected components implemented, and it seems like its
    // just an intermediate step to make sure the map computation uses their
    // connected components algorithm rather than the traditional map algorithms.

  }

  //WORKLOAD ITEM 10
  /**
    * Credits: Daniel Marcous (https://github.com/dmarcous/spark-betweenness/blob/master/src/main/scala/com/centrality/kBC/KBetweenness.scala)
    * Computes the betweenness centrality of a graph given a max k value
    *
    * @param graph The input graph
    * @param k The maximum number of hops to compute
    * @tparam VD Node attribute type for input graph
    * @tparam ED Edge attribute type for input graph
    * @return Graph containing the betweenness double values
    */
  def betweennessCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], k: Int):
    Graph[Double, Double] = KBetweenness.run(graph, k)

  //WORKLOAD ITEM 12
  def subGraphIsoMorphism[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], k: Int) = {
    // This one is gonna take some time for me to wrap my head around and find a way
    // to implement it in spark.
  }

}

