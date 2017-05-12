package edu.msstate.dasi.csb

/**
 * Provides factory methods for each benchmark component, defining their behavior from an input configuration.
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
      case "ba" => new ParallelBaSynth(config.partitions, config.iterations, config.sampleFraction)
      case "kro" => new KroSynth(config.partitions, config.seedMatrix, config.iterations)
    }
  }


  def getMetrics: Array[Veracity] = {
    var metrics = Array.empty[Veracity]

    if (config.metrics.isEmpty) return metrics

    val all = config.metrics.contains("all")

    if (all || config.metrics.contains("degree")) metrics :+= DegreeVeracity
    if (all || config.metrics.contains("in-degree")) metrics :+= InDegreeVeracity
    if (all || config.metrics.contains("out-degree")) metrics :+= OutDegreeVeracity
    if (all || config.metrics.contains("pagerank")) metrics :+= PageRankVeracity

    metrics
  }

  /**
   * Returns the workload engine.
   */
  def getWorkloadEngine: Workload = {
    config.workloadBackend match {
      case "spark" => SparkWorkload
      case "neo4j" => new Neo4jWorkload(config.neo4jUrl, config.neo4jUsername, config.neo4jPassword)
    }
  }
}
