package edu.msstate.dasi.csb

class ComponentFactory(config: Config) {

  /**
   *
   */
  def getLoader: GraphPersistence = {
    config.graphLoader match {
      case "spark" => SparkPersistence
      case "neo4j" => Neo4jPersistence
    }
  }

  /**
   *
   */
  def getSaver: GraphPersistence = {
    config.graphSaver match {
      case "spark" => SparkPersistence
      case "neo4j" => Neo4jPersistence
    }
  }

  /**
   *
   */
  def getTextSaver: Option[GraphPersistence] = {
    config.textSaver match {
      case "spark" => Some(SparkPersistence)
      case "neo4j" => Some(Neo4jPersistence)
      case "none" => None
    }
  }

  /**
   *
   */
  def getSynthesizer: GraphSynth = {
    config.synthesizer match {
      case "ba" => new ParallelBaSynth(config.partitions, config.iterations, config.sampleFraction)
      case "kro" => new KroSynth(config.partitions, config.seedMatrix, config.iterations)
    }
  }

  /**
   *
   */
  def getWorkload: Workload = {
    config.workloadBackend match {
      case "spark" => SparkWorkload
      case "neo4j" => new Neo4jWorkload(config.neo4jUrl, config.neo4jUsername, config.neo4jPassword)
    }
  }
}
