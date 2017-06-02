package edu.msstate.dasi.csb

import edu.msstate.dasi.csb.model.EdgeData
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.reflect.ClassTag

trait Workload {
  /**
   * The number of vertices in the graph.
   */
  def countVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit

  /**
   * The number of edges in the graph.
   */
  def countEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit

  /**
   * The degree of each vertex in the graph.
   * @note Vertices with no edges are not considered.
   */
  def degree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit

  /**
   * The in-degree of each vertex in the graph.
   * @note Vertices with no incoming edges are not considered.
   */
  def inDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit

  /**
   * The out-degree of each vertex in the graph.
   * @note Vertices with no outgoing edges are not considered.
   */
  def outDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   */
  def pageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], tol: Double = 0.001, resetProb: Double = 0.15): Unit

  /**
   * Breadth-first Search: returns the shortest directed-edge path from src to dst in the graph. If no path exists,
   * returns the empty list.
   */
  def bfs[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Unit

  /**
   * Collects list of neighbors based solely on incoming direction, and returns a list of
   * those neighbors as well as their node attribute
   * @param graph The input graph
   *
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def inNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Unit

  /**
   * Collects list of neighbors based solely on outgoing direction, and returns a list of
   * those neighbors as well as their node attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def outNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Unit

  /**
   * Collects list of neighbors in both incoming and outgoing direction, and returns a list of
   * those neighbors as well as their node attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def neighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Unit

  /**
   * Grabs all of the edges entering a node by grouping the edges by dstId attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
   */
  def inEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Unit

  /**
   * Grabs all of the edges exiting a node by grouping the edges by srcId attribute
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
   */
  def outEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Unit

  /**
   * Computes the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   */
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], maxIterations: Int = Int.MaxValue): Unit

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   */
  def stronglyConnectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], numIter: Int): Unit

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def triangleCount[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Unit

  /**
   * Computes the betweenness centrality of a graph given a max k value.
   *
   * @param graph The input graph
   * @param k The maximum number of hops to compute
   * @return Graph containing the betweenness double values
   */
  def betweennessCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Unit

  /**
   * Computes the closeness centrality of a node using the formula N/(sum(distances)).
   */
  def closenessCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertex: VertexId): Unit

  /**
   * Computes the shortest path from a source vertex to all other vertices.
   */
  def sssp[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId): Unit

  /**
   * Finds all edges with a given property.
   */
  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], filter: Edge[EdgeData] => Boolean): Unit

  /**
   * Finds one or more subgraphs of the graph which are isomorphic to the pattern.
   */
  def subgraphIsomorphism[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], pattern: Graph[VD, ED]): Unit
}
