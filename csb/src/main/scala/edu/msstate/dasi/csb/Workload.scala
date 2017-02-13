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

  /**
   * Breadth-first Search: returns the shortest directed-edge path from src to dst in the graph. If no path exists,
   * returns the empty list.
   */
  def bfs[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Seq[VertexId] = {
    if (src == dst) return List(src)

    // The attribute of each vertex is (dist from src, id of vertex with dist-1)
    var g: Graph[(Int, VertexId), ED] =
      graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L)).cache()

    // Traverse forward from src
    var dstAttr = (Int.MaxValue, 0L)
    while (dstAttr._1 == Int.MaxValue) {
      val msgs = g.aggregateMessages[(Int, VertexId)](
        e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
          e.sendToDst((e.srcAttr._1 + 1, e.srcId))
        },
        (a, b) => if (a._1 < b._1) a else b).cache()

      if (msgs.count == 0) return List.empty

      g = g.ops.joinVertices(msgs) {
        (id, oldAttr, newAttr) =>
          if (newAttr._1 < oldAttr._1) newAttr else oldAttr
      }.cache()

      dstAttr = g.vertices.filter(_._1 == dst).first()._2
    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

    path
  }

  /**
   * Collects list of neighbors based solely on incoming direction, and returns a list of
   * those neighbors as well as their node attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def inNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(EdgeDirection.In)
  }

  /**
   * Collects list of neighbors based solely on outgoing direction, and returns a list of
   * those neighbors as well as their node attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def outNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(EdgeDirection.Out)
  }

  /**
   * Collects list of neighbors in both incoming and outgoing direction, and returns a list of
   * those neighbors as well as their node attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def neighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(EdgeDirection.Both)
  }

  /**
   * Grabs all of the edges entering a node by grouping the edges by dstId attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
   */
  def inEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): RDD[(VertexId, Iterable[Edge[ED]])] = {
    graph.edges.groupBy(record => record.dstId)
  }

  /**
   * Grabs all of the edges exiting a node by grouping the edges by srcId attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
   */
  def outEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): RDD[(VertexId, Iterable[Edge[ED]])] = {
    graph.edges.groupBy(record => record.srcId)
  }

  /**
   * Computes the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   */
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], maxIterations: Int = 0): Graph[VertexId, ED] = {
    if (maxIterations > 0) {
      graph.connectedComponents(maxIterations)
    } else {
      graph.connectedComponents()
    }
  }

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   */
  def stronglyConnectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], numIter: Int): Graph[VertexId, ED] = {
    graph.stronglyConnectedComponents(numIter)
  }

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def triangleCount[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Graph[Int, ED] = {
    graph.triangleCount()
  }

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
  def betweennessCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Graph[Double, Double] = {
    KBetweenness.run(graph, k)
  }

  def subGraphIsoMorphism[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int) = {
    // This one is gonna take some time for me to wrap my head around and find a way
    // to implement it in spark.
  }

  private def edgeWithPropFilter(edge: Edge[EdgeData], filterEdge: Edge[EdgeData]): Boolean = {
    filterEdge.attr ~= edge.attr
  }

  private def edgeWithPropRangeFilter(edge: Edge[EdgeData], min: Edge[EdgeData], max: Edge[EdgeData]): Boolean = {
    edge.attr < max.attr && min.attr < edge.attr
  }

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], property: EdgeData): RDD[Edge[EdgeData]] = {
    val filterEdge = Edge(0L, 0L, property)
    graph.edges.filter(edge => edgeWithPropFilter(edge, filterEdge))
  }

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], min: EdgeData, max: EdgeData): RDD[Edge[EdgeData]] = {
    val filterMin = Edge(0L, 0L, min)
    val filterMax = Edge(0L, 0L, max)
    graph.edges.filter(edge => edgeWithPropRangeFilter(edge, filterMin, filterMax))
  }
}

