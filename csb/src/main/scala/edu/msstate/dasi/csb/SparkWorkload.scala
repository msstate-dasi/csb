package edu.msstate.dasi.csb

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SparkWorkload extends Workload {
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

  /***
    * Returns the closeness centrality of a node using the formula N/(sum(distances)).
    * @param vertex The vertext to calculate centraility
    * @param graph the graph the vertex exists in
    * @tparam VD reflection.  We don't care about the data
    * @tparam ED reflection.  We don't care about the data
    * @return
    */
  def closenessCentrality[VD: ClassTag, ED: ClassTag](vertex: VertexId, graph: Graph[VD, ED]): Double =
  {
    return ClosenessCentrality.getClosenessOfVert(vertex, graph)
  }

  /***
    * This returns the shortest path from srcVertex to destVertex.  By returning in this case we mean returning a list of the vertexId's from srcVertex to destVertex by following the least number of edges possible.
    * @param graph the graph
    * @param srcVertex source vertex
    * @param destVertex destination vertex
    * @tparam VD reflection.  We don't care about the property that could be here
    * @tparam ED reflection.  We don't care about the property that could be here
    * @return
    */
  def singleSourceShortestPathSeq[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], srcVertex: VertexId, destVertex: VertexId): Seq[VertexId] =
  {
    return bfs(graph, srcVertex, destVertex)
  }

  /***
    * The same as the other SSSP but we return the number of hops it takes to go from the src node to dest node
    * @param graph
    * @param srcVertex
    * @param destVertex
    * @tparam VD
    * @tparam ED
    * @return
    */
  def singleSourceShortestPathNum[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], srcVertex: VertexId, destVertex: VertexId): Long =
  {
    return bfs(graph, srcVertex, destVertex).size - 1
  }

  /**
    * Finds all edges with a given property.
    */
  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], property: EdgeData): RDD[Edge[EdgeData]] = {
    graph.edges.filter(edge => property ~= edge.attr)
  }

  /**
    * Finds all edges with a given property range.
    */
  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], propertyMin: EdgeData, propertyMax: EdgeData): RDD[Edge[EdgeData]] = {
    if(propertyMin > propertyMax || !(propertyMin < propertyMax || propertyMax < propertyMin)) throw new IllegalArgumentException("propertyMin MUST be lower for all values of Edge data"); //If the properties are not set correctly then this function will output nonsense.
    graph.edges.filter(edge => propertyMax > edge.attr && propertyMin < edge.attr)
  }
}
