package edu.msstate.dasi.csb.workload.neo4j

import edu.msstate.dasi.csb.workload.Workload
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * PageRank algorithm implementation.
 */
class PageRank(engine: Neo4jEngine) extends Workload {
  val name = "PageRank"

  /**
   * Runs PageRank.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Unit = {
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

    engine.run(query)
  }
}
