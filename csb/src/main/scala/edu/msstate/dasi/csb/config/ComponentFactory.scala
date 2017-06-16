package edu.msstate.dasi.csb.config

import edu.msstate.dasi.csb.data.synth.{GraphSynth, ParallelBa, StochasticKronecker}
import edu.msstate.dasi.csb.persistence.{GraphPersistence, Neo4jPersistence, SparkPersistence}
import edu.msstate.dasi.csb.veracity._
import edu.msstate.dasi.csb.workload._

/**
 * Provides factory methods for each benchmark component, defining their behavior from an input configuration.
 *
 * @param config the input configuration
 */
class ComponentFactory(config: Config) {

  /**
   * Returns the graph loader.
   */
  def getLoader: GraphPersistence = {
    config.graphLoader match {
      case "spark" => SparkPersistence
      case "neo4j" => Neo4jPersistence
    }
  }

  /**
   * Returns the graph saver.
   */
  def getSaver: Option[GraphPersistence] = {
    config.graphSaver match {
      case "spark" => Some(SparkPersistence)
      case "neo4j" => Some(Neo4jPersistence)
      case "none" => None
    }
  }

  /**
   * Returns the graph saver used for the text format.
   */
  def getTextSaver: Option[GraphPersistence] = {
    config.textSaver match {
      case "spark" => Some(SparkPersistence)
      case "neo4j" => Some(Neo4jPersistence)
      case "none" => None
    }
  }

  /**
   * Returns the synthesizer.
   */
  def getSynthesizer: GraphSynth = {
    config.synthesizer match {
      case "ba" => new ParallelBa(config.partitions, config.iterations, config.sampleFraction)
      case "kro" => new StochasticKronecker(config.partitions, config.seedMatrix, config.iterations)
    }
  }

  /**
   * Returns the veracity metrics.
   */
  def getMetrics: Array[Veracity] = {
    var metrics = Array.empty[Veracity]

    if (config.metrics.isEmpty) return metrics

    val all = config.metrics.contains("all")

    if (all || config.metrics.contains("degree")) metrics :+= Degree
    if (all || config.metrics.contains("in-degree")) metrics :+= InDegree
    if (all || config.metrics.contains("out-degree")) metrics :+= OutDegree
    if (all || config.metrics.contains("pagerank")) metrics :+= PageRank

    metrics
  }

  private def getSparkWorkloads(all: Boolean): Array[Workload] = {
    var workloads = Array.empty[Workload]

    val engine = new spark.SparkEngine

    if ( all || config.workloads.contains("count-vertices") ) workloads :+= new spark.CountVertices(engine)
    if ( all || config.workloads.contains("count-edges") ) workloads :+= new spark.CountEdges(engine)
    if ( all || config.workloads.contains("degree") ) workloads :+= new spark.Degree(engine)
    if ( all || config.workloads.contains("in-degree") ) workloads :+= new spark.InDegree(engine)
    if ( all || config.workloads.contains("out-degree") ) workloads :+= new spark.OutDegree(engine)
    if ( all || config.workloads.contains("pagerank") ) workloads :+= new spark.PageRank(engine)
    if ( all || config.workloads.contains("neighbors") ) workloads :+= new spark.Neighbors(engine)
    if ( all || config.workloads.contains("in-neighbors") ) workloads :+= new spark.Neighbors(engine)
    if ( all || config.workloads.contains("out-neighbors") ) workloads :+= new spark.Neighbors(engine)
    if ( all || config.workloads.contains("in-edges") ) workloads :+= new spark.InEdges(engine)
    if ( all || config.workloads.contains("out-edges") ) workloads :+= new spark.OutEdges(engine)
    if ( all || config.workloads.contains("connected-components") ) workloads :+= new spark.ConnectedComponents(engine)
    if ( all || config.workloads.contains("triangle-count") ) workloads :+= new spark.TriangleCounting(engine)

    if ( all || config.workloads.contains("strongly-connected-components") ) {
      workloads :+= new spark.StronglyConnectedComponents(engine, config.iterations)
    }
    if ( all || config.workloads.contains("betweenness-centrality") ) {
      workloads :+= new spark.BetweennessCentrality(engine, config.iterations)
    }
    if ( all || config.workloads.contains("bfs") ) {
      workloads :+= new spark.BFS(engine, config.srcVertex, config.dstVertex)
    }
    if ( all || config.workloads.contains("closeness-centrality") ) {
      workloads :+= new spark.ClosenessCentrality(engine, config.srcVertex)
    }
    if ( all || config.workloads.contains("sssp") ) {
      workloads :+= new spark.SSSP(engine, config.srcVertex)
    }
    if ( all || config.workloads.contains("subgraph-isomorphism") ) {
      val pattern = this.getLoader.loadGraph(config.patternPrefix, config.partitions)
      workloads :+= new spark.SubgraphIsomorphism(engine, pattern)
    }

    workloads
  }

  private def getNeo4jWorkloads(all: Boolean): Array[Workload] = {
    var workloads = Array.empty[Workload]

    val engine = new neo4j.Neo4jEngine(config.neo4jUrl, config.neo4jUsername, config.neo4jPassword)

    if ( all || config.workloads.contains("count-vertices") ) workloads :+= new neo4j.CountVertices(engine)
    if ( all || config.workloads.contains("count-edges") ) workloads :+= new neo4j.CountEdges(engine)
    if ( all || config.workloads.contains("degree") ) workloads :+= new neo4j.Degree(engine)
    if ( all || config.workloads.contains("in-degree") ) workloads :+= new neo4j.InDegree(engine)
    if ( all || config.workloads.contains("out-degree") ) workloads :+= new neo4j.OutDegree(engine)
    if ( all || config.workloads.contains("pagerank") ) workloads :+= new neo4j.PageRank(engine)
    if ( all || config.workloads.contains("neighbors") ) workloads :+= new neo4j.Neighbors(engine)
    if ( all || config.workloads.contains("in-neighbors") ) workloads :+= new neo4j.Neighbors(engine)
    if ( all || config.workloads.contains("out-neighbors") ) workloads :+= new neo4j.Neighbors(engine)
    if ( all || config.workloads.contains("in-edges") ) workloads :+= new neo4j.InEdges(engine)
    if ( all || config.workloads.contains("out-edges") ) workloads :+= new neo4j.OutEdges(engine)
    if ( all || config.workloads.contains("connected-components") ) workloads :+= new neo4j.ConnectedComponents(engine)
    if ( all || config.workloads.contains("triangle-count") ) workloads :+= new neo4j.TriangleCounting(engine)

    if ( all || config.workloads.contains("strongly-connected-components") ) {
      workloads :+= new neo4j.StronglyConnectedComponents(engine)
    }
    if ( all || config.workloads.contains("betweenness-centrality") ) {
      workloads :+= new neo4j.BetweennessCentrality(engine, config.iterations)
    }
    if ( all || config.workloads.contains("bfs") ) {
      workloads :+= new neo4j.BFS(engine, config.srcVertex, config.dstVertex)
    }
    if ( all || config.workloads.contains("closeness-centrality") ) {
      workloads :+= new neo4j.ClosenessCentrality(engine, config.srcVertex)
    }
    if ( all || config.workloads.contains("sssp") ) {
      workloads :+= new neo4j.SSSP(engine, config.srcVertex)
    }
    if ( all || config.workloads.contains("subgraph-isomorphism") ) {
      workloads :+= new neo4j.SubgraphIsomorphism(engine)
    }

    workloads
  }

  /**
   * Returns the workloads.
   */
  def getWorkloads: Array[Workload] = {
    val all = config.workloads.contains("all")

    config.workloadBackend match {
      case "spark" => getSparkWorkloads(all)
      case "neo4j" => getNeo4jWorkloads(all)
    }
  }
}
