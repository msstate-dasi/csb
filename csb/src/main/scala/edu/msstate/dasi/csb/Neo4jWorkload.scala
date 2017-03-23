package edu.msstate.dasi.csb

import java.util.concurrent.TimeUnit

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.summary.ResultSummary
import org.neo4j.driver.v1.{AccessMode, AuthTokens, GraphDatabase}

import scala.reflect.ClassTag

object Neo4jWorkload extends Workload {
  /**
   * The Neo4j driver instance responsible for obtaining new sessions.
   */
  private val driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "password"))

  /**
   *
   */
  private def printSummary(querySummary: ResultSummary): Unit = {
    val timeAfter = querySummary.resultAvailableAfter(TimeUnit.MILLISECONDS) / 1000.0
    val timeConsumed = querySummary.resultConsumedAfter(TimeUnit.MILLISECONDS) / 1000.0

    println(s"[NEO4J] Query completed in $timeAfter s")
    println(s"[NEO4J] Result consumed in $timeConsumed s")
    println(s"[NEO4J] Execution completed in ${timeAfter + timeConsumed} s")
  }

  /**
   *
   */
  private def run(query: String, accessMode: AccessMode = AccessMode.READ): Unit = {
    val session = driver.session(accessMode)

    val result = session.run(query)

    while ( result.hasNext ) result.next()

    printSummary( result.consume() )

    session.close()
  }

  /**
   * The number of vertices in the graph.
   */
  def countVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH () RETURN count(*);"

    run(query)
  }

  /**
   * The number of edges in the graph.
   */
  def countEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH ()-->() RETURN count(*);"

    run(query)
  }

  /**
   * The degree of each vertex in the graph.
   * @note Vertices with no edges not considered.
   */
  def degree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)--() RETURN n, count(*);"

    run(query)
  }

  /**
   * The in-degree of each vertex in the graph.
   * @note Vertices with no incoming edges are not considered.
   */
  def inDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)<--() RETURN n, count(*);"

    run(query)
  }

  /**
   * The out-degree of each vertex in the graph.
   * @note Vertices with no outgoing edges are not considered.
   */
  def outDegree[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)-->() RETURN n, count(*);"

    run(query)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   */
  def pageRank[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], tol: Double = 0.001, resetProb: Double): Unit = {
    val query = "MATCH (a) " +
      "set a.pagerank = 0.0 " +
      "WITH collect(distinct a) AS nodes,count(a) as num_nodes " +
      "UNWIND nodes AS a " +
      "MATCH (a)-[r]-(b) " +
      "WITH a,collect(r) AS rels, count(r) AS num_rels, 1.0/num_nodes AS rank " +
      "UNWIND rels AS rel " +
        "SET endnode(rel).pagerank = " +
      "CASE " +
      "WHEN num_rels > 0 AND id(startnode(rel)) = id(a) THEN " +
        "endnode(rel).pagerank + rank/(num_rels) " +
      "ELSE endnode(rel).pagerank " +
      "END " +
      ",startnode(rel).pagerank = " +
      "CASE " +
      "WHEN num_rels > 0 AND id(endnode(rel)) = id(a) THEN " +
        "startnode(rel).pagerank + rank/(num_rels) " +
      "ELSE startnode(rel).pagerank " +
      "END " +
      "WITH collect(distinct a) AS a,rank " +
      "RETURN a"

    run(query)
  }

  /**
   * Breadth-first Search: returns the shortest directed-edge path from src to dst in the graph. If no path exists,
   * returns the empty list.
   */
  def bfs[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Unit = {
    val query = "MATCH path = shortestPath(({name:\"" + src + "\"})-[*]-({name:\"" + dst + "\"})) RETURN path;"

    run(query)
  }

  /**
   * Collects list of neighbors based solely on incoming direction, and returns a list of
   * those neighbors as well as their node attribute
   *
   * @param graph The input graph
   *
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def inNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n) RETURN n, [ (n)<--(m) | m ];"

    run(query)
  }

  /**
   * Collects list of neighbors based solely on outgoing direction, and returns a list of
   * those neighbors as well as their node attribute
   *
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   *
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def outNeighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n) RETURN n, [ (n)-->(m) | m ];"

    run(query)
  }

  /**
   * Collects list of neighbors in both incoming and outgoing direction, and returns a list of
   * those neighbors as well as their node attribute
   *
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   *
   * @return RDD of Arrays which contain VertexId and VD for each neighbor
   */
  def neighbors[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n) RETURN n, [ (n)--(m) | m ];"

    run(query)
  }

  /**
   * Grabs all of the edges entering a node by grouping the edges by dstId attribute
   *
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   *
   * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
   */
  def inEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n) RETURN n, [ (n)<-[r]-() | r ];"

    run(query)
  }

  /**
   * Grabs all of the edges exiting a node by grouping the edges by srcId attribute
   *
   * @param graph The input graph
   * @tparam VD Node attribute type for input graph
   * @tparam ED Edge attribute type for input graph
   *
   * @return RDD containing pairs of (VertexID, Iterable of Edges) for every vertex in the graph
   */
  def outEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n) RETURN n, [ (n)-[r]->() | r ];"

    run(query)
  }

  /**
   * Computes the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   */
  def connectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxIterations: Int = Int.MaxValue): Unit = {
    val query = "MATCH (n) WITH COLLECT(n) as nodes " +
      "RETURN REDUCE(graphs = [], n in nodes | " +
      "case when " +
      "ANY (g in graphs WHERE shortestPath( (n)-[*]-(g) ) ) " +
      "then graphs " +
      "else graphs + [n]" +
      "end );"

    run(query)
  }

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   */
  def stronglyConnectedComponents[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Unit = {
    val query = "MATCH (n) " +
      "WITH COLLECT(n) as nodes " +
      "RETURN REDUCE(graphs = [], n in nodes | " +
      "case when " +
      "ANY (g in graphs WHERE (shortestPath( (n)-[*]->(g) ) AND shortestPath( (n)<-[*]-(g) ) ) ) " +
      "then graphs " +
      "else graphs + [n] " +
      "end ) "

    run(query)
  }

  /**
   * Computes the number of triangles passing through each vertex.
   */
  def triangleCount[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = "MATCH (n)-->()-->()-->(n) RETURN n, count(*);"

    run(query)
  }

  /**
   * Computes the betweenness centrality of a graph given a max k value
   *
   * @param graph The input graph
   * @param k     The maximum number of hops to compute
   *
   * @return Graph containing the betweenness double values
   */
  def betweennessCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Unit = {
    val query = s"MATCH (n), pthroughn = shortestPath((a)-[*..$k]->(b)) " +
      "WHERE n IN nodes(pthroughn) AND n <> a AND n <> b AND a <> b " +
      "WITH n,a,b,count(pthroughn) AS sum " +
      s"MATCH p = shortestPath((a)-[*..$k]->(b)) " +
      "WITH n, a, b, tofloat(sumn)/ tofloat(count(p)) AS fraction " +
      "RETURN n, sum(fraction);"

    run(query)
  }

  /**
   * Computes the closeness centrality of a node using the formula N/(sum(distances)).
   */
  def closenessCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertex: VertexId): Double = ???

  /**
   * Computes the shortest path from a source vertex to all other vertices.
   */
  def sssp[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], src: VertexId): Unit = {
    val query ="MATCH (src {name:\"" + src + "\"}), (dst)," +
      "path = shortestPath((src)-[*]->(dst))" +
      "RETURN src, dst, path;"

    run(query)
  }

  /**
   * Finds all edges with a given property.
   */
  def edgesWithProperty[VD: ClassTag](graph: Graph[VD, EdgeData], filter: Edge[EdgeData] => Boolean): RDD[Edge[EdgeData]] = ???
}
