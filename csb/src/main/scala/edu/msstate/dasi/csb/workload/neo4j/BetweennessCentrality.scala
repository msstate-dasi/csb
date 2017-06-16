package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Betweenness Centrality algorithm implementation.
 *
 * @param hops the maximum number of hops
 */
class BetweennessCentrality(engine: Neo4jEngine, hops: Int) extends Workload {
  val name = "Betweenness Centrality"

  /**
   * Runs Betweenness Centrality.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
    val query = s"MATCH (n), pthroughn = shortestPath((a)-[*..$hops]->(b)) " +
      "WHERE n IN nodes(pthroughn) AND n <> a AND n <> b AND a <> b " +
      "WITH n,a,b,count(pthroughn) AS sumn " +
      s"MATCH p = shortestPath((a)-[*..$hops]->(b)) " +
      "WITH n, a, b, tofloat(sumn)/ tofloat(count(p)) AS fraction " +
      "RETURN n, sum(fraction);"

    engine.run(query)
  }
}
