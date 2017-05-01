package edu.msstate.dasi.csb

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph

//import scopt.OptionParser

object Benchmark {

  val versionString = "0.2-DEV"

//  case class ParamsHelp(
//                         /**
//                           * Any Arguments
//                           */
//                         outputGraphPrefix_desc: String = "Prefix to use when saving the output graph",
//                         partitions_desc: String = "Number of partitions to set RDDs to.",
//                         backend_desc: String = "Backend used to save generated data (fs or neo4j).",
//                         checkpointDir_desc: String = "Directory for checkpointing intermediate results. Checkpointing helps with recovery and eliminates temporary shuffle files on disk.",
//                         checkpointInterval_desc: String = "Iterations between each checkpoint. Only used if checkpointDir is set.",
//                         seed_desc: String = "Path of the seed.",
//
//                         /**
//                           * GenDist Arguments
//                           */
//                         connLog_desc: String = "Bro IDS conn.log file to augment with a SNORT alert.log.",
//                         alertLog_desc: String = "SNORT alert augment with a Bro IDS conn.log.",
//                         augLog_desc: String = "Augmented Bro IDS conn.log and SNORT alert.log.",
//
//                         /**
//                           * BA Arguments
//                           */
//                         noProp_desc: String = "Specify whether to generate random properties during generation or not.",
//                         fractionPerIter_desc: String = "The fraction of vertices to add to the graph per iteration.",
//                         baIter_desc: String = "Number of iterations for Barabasi–Albert model.",
//
//                         /**
//                           * Kronecker Arguments
//                           */
//                         seedMtx_desc: String = "Space-separated matrix file to use as a seed for Kronecker.",
//                         kroIter_desc: String = "Number of iterations for Kronecker model.",
//
//                         /**
//                           * veracity arguements
//                           */
//                         veracity_Desc: String = "The veracity metric you want to compute. Options include: degree, inDegree, outDegree, pageRank.",
//                         veracity_File: String = "The file to save the metric information.",
//                         seed_Metric: String = "Serialized file to use as a seed.",
//                         synth_Metric: String = "Serialized file to use as a synth.",
//
//                         /**
//                           * Workload Arguments
//                           */
//                         graph: String = "The input graph."
//                       )

  case class Params(
                     mode: String = "",

                     /**
                       * Any Arguments
                       */
                     outputGraphPrefix: String = "",
                     partitions: Int = 120,
                     backend: String = "fs",
                     checkpointDir: Option[String] = None,
                     checkpointInterval: Int = 10,
                     debug: Boolean = false,

                     /**
                       * GenDist Arguments
                       */
                     connLog: String = "conn.log",
                     alertLog: String = "alert",
                     augLog: String = "aug.log",

                     /**
                       * BA Arguments
                       */
                     noProp: Boolean = false,
                     fractionPerIter: Double = 0.1,
                     seed: String = "seed",
                     baIter: Long = 1000,

                     /**
                       * Kronecker Arguments
                       */
                     seedMtx: String = "seed.mtx",
                     kroIter: Int = 10,

                     /**
                       * Veracity Arguments
                       */
                     metric: String = "hop-plot",
                     metricSave: String = "hop-plotSave",
                     synth: String = "synth",

                     /**
                       * Workload Arguments
                       */
                     graph: String = ""
                   )


  def main(args: Array[String]) {


//    val dP = Params()
//    val h = ParamsHelp()
//
//    val parser = new OptionParser[Params]("Benchmark") {
//      head(s"Benchmark $versionString: a synthetic Graph Generator for the busy scientist.")
//
//      /**
//        * All Arguments:
//        */
//      opt[String]("output")
//        .text(s"${h.outputGraphPrefix_desc} Default ${dP.outputGraphPrefix}")
//        .action((x, c) => c.copy(outputGraphPrefix = x))
//      opt[Int]("partitions")
//        .text(s"${h.partitions_desc} Default: ${dP.partitions}")
//        .validate(x => if (x > 0) success else failure("Partition count must be greater than 0."))
//        .action((x, c) => c.copy(partitions = x))
//      opt[String]("backend")
//        .text(s"${h.backend_desc} Default: ${dP.backend}")
//        .validate(x => if (x == "fs" || x == "neo4j") success else failure("Backend must be fs or neo4j."))
//        .action((x, c) => c.copy(backend = x))
//      opt[String]("checkpointDir")
//        .text(s"${h.checkpointDir_desc} Default: ${dP.checkpointDir}")
//        .action((x, c) => c.copy(checkpointDir = Some(x)))
//      opt[Int]("checkpointInterval")
//        .text(s"${h.checkpointInterval_desc} Default: ${dP.checkpointInterval}")
//        .action((x, c) => c.copy(checkpointInterval = x))
//      opt[Unit]("debug")
//        .hidden()
//        .action((_, c) => c.copy(debug = true))
//        .text(s"Debug mode, prints all log output to terminal. default: ${dP.debug}")
//
//      /**
//        * GenDist Arguments:
//        */
//      note("\n")
//      cmd("gen_dist").action((_, c) => c.copy(mode = "gen_dist"))
//        .text(s"Generate distribution data for a given input dataset.")
//        .children(
//          arg[String]("bro_log")
//            .text(s"${h.connLog_desc} Default: ${dP.connLog}")
//            .required()
//            .action((x, c) => c.copy(connLog = x)),
//          arg[String]("alert_log")
//            .text(s"${h.alertLog_desc} Default: ${dP.alertLog}")
//            .required()
//            .action((x, c) => c.copy(alertLog = x)),
//          arg[String]("aug_log")
//            .text(s"${h.augLog_desc} Default: ${dP.augLog}")
//            .required()
//            .action((x, c) => c.copy(augLog = x)),
//          arg[String]("seed")
//            .text(s"${h.seed_desc} Default: ${dP.seed}")
//            .required()
//            .action((x, c) => c.copy(seed = x))
//        )
//
//      /**
//        * BA Arguments:
//        */
//      note("\n")
//      cmd("ba").action((_, c) => c.copy(mode = "ba"))
//        .text(s"Generate synthetic graph using the Barabasi–Albert model.")
//        .children(
//          opt[Unit]("no-prop")
//            .text(s"${h.noProp_desc} default: ${dP.noProp}")
//            .action((_, c) => c.copy(noProp = true)),
//          opt[Double]("fraction-per-iter")
//            .text(s"${h.fractionPerIter_desc} default: ${dP.fractionPerIter}")
//            .action((x, c) => c.copy(fractionPerIter = x)),
//          arg[String]("seed")
//            .text(s"${h.seed_desc} default: ${dP.seed}")
//            .required()
//            .action((x, c) => c.copy(seed = x)),
//          arg[Int]("<# of Iterations>")
//            .text(s"${h.baIter_desc} default: ${dP.baIter}")
//            .validate(x => if (x > 0) success else failure("Iteration count must be greater than 0."))
//            .action((x, c) => c.copy(baIter = x))
//        )
//
//      /**
//        * Kronecker Arguments
//        */
//      note("\n")
//      cmd("kro").action((_, c) => c.copy(mode = "kro"))
//        .text(s"Generate synthetic graph using the Probabilistic Kronecker model.")
//        .children(
//          opt[Unit]("no-prop")
//            .text(s"${h.noProp_desc} default ${dP.noProp}")
//            .action((_, c) => c.copy(noProp = true)),
//          arg[String]("seed-mtx")
//            .text(s"${h.seedMtx_desc} default: ${dP.seedMtx}")
//            .required()
//            .action((x, c) => c.copy(seedMtx = x)),
//          arg[String]("seed")
//            .text(s"${h.seed_desc} default: ${dP.seed}")
//            .required()
//            .action((x, c) => c.copy(seed = x)),
//          arg[Int]("<# of Iterations>")
//            .text(s"${h.kroIter_desc} default: ${dP.kroIter}")
//            .validate(x => if (x > 0) success else failure("Iteration count must be greater than 0."))
//            .action((x, c) => c.copy(kroIter = x))
//        )
//
//      /**
//       * Veracity Arguments
//       */
//      note("\n")
//      cmd("ver").action((_, c) => c.copy(mode = "ver"))
//        .text(s"Compute veracity metrics on a given vertices and edge seed files")
//        .children(
//          arg[String]("seed")
//              .text(s"${h.seed_Metric} default: ${dP.seed}")
//              .required()
//              .action((x,c) => c.copy(seed = x)),
//          arg[String]("synth")
//              .text(s"${h.synth_Metric}")
//              .required()
//              .action((x,c) => c.copy(synth = x)),
//          arg[String]("metric")
//            .text(s"${h.veracity_Desc}")
//            .required()
//            .action((x,c) => c.copy(metric = x)),
//          arg[String]("save_file")
//            .text(s"${h.veracity_File}")
//            .required()
//            .action((x,c) => c.copy(metricSave = x))
//        )
//
//      /**
//       * Workload Arguments
//       */
//      note("\n")
//      cmd("workload").action((_, c) => c.copy(mode = "workload"))
//        .text(s"Execute the workloads on an existing graph")
//        .children(
//          arg[String]("graph")
//              .text(s"${h.graph} default: ${dP.graph}")
//              .required()
//              .action((x,c) => c.copy(graph = x))
//        )
//
//    }
//
//    parser.parse(args, dP) match {
//      case Some(config) => if (config.mode != "") run(config)
//      else {
//        println("Error: Must specify command")
//        parser.showUsageAsError()
//      }
//      case _ => sys.exit(1)
//    }

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val parser = new OptionParser("csb", versionString, Config())

    parser.parse(args) match {
      case Some(config) =>
//        if ( ! config.debug ) {
          //turn off annoying log messages

//        } else {
//
//        }

        config.mode match {
          case "seed" => run_gendist(config)
          case "synth" => run_synth(config)
          case "veracity" => run_veracity(config)
          case "workload" => println("workload")
        }
      case None => sys.exit(1)
    }
  }

  /** * Main function of our program, controls graph generation and other pieces.
    *
    * @param params Parameters for the function to run.
    */
  def run(params: Params): Boolean = {
    if ( ! params.debug ) {
      //turn off annoying log messages
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    } else {

    }

    params.mode match {
//      case "gen_dist" => run_gendist(config)
//      case "ba" => run_synth(params)
//      case "kro" => run_synth(params)
//      case "ver" => run_ver(params)
      case "workload" => run_workload(params)
      case _ => sys.exit(1)
    }

    sys.exit()
    true
  }

  def run_gendist(config: Config): Boolean = {
    //these two statements create the aug log (conn.log plus alert)
    val logAug = new log_Augment()
    logAug.run(config.alertLog, config.connLog, config.outLog)

    val seed = Util.time( "Log to graph", {
      val seed = DataParser.logToGraph(config.outLog, config.partitions)
      println("Vertices #: " + seed.numVertices + ", Edges #: " + seed.numEdges)
      seed
    } )

    Util.time( "Seed distributions", new DataDistributions(config.outLog) )

    var graphPs = null.asInstanceOf[GraphPersistence]
    config.backend match {
      case "fs" => graphPs = new SparkPersistence()
      case "neo4j" => graphPs = new Neo4jPersistence()
    }

    Util.time( "Save seed graph", graphPs.saveGraph(seed, config.seedGraphPrefix, overwrite = true) )

    true
  }

  private def run_metrics(metrics: Seq[String], seed: Graph[VertexData, EdgeData], synth: Graph[VertexData, EdgeData]): Unit = {
    if ( metrics.isEmpty || metrics.contains("none") ) return

    val all = metrics.contains("all")

    if ( all || metrics.contains("degree") ) {
      val degVeracity = Util.time("Degree Veracity", DegreeVeracity(seed, synth))
      println(s"Degree Veracity: $degVeracity")
    }

    if ( all || metrics.contains("in-degree") ) {
      val inDegVeracity = Util.time("In-Degree Veracity", InDegreeVeracity(seed, synth))
      println(s"In-Degree Veracity: $inDegVeracity")
    }

    if ( all || metrics.contains("out-degree") ) {
      val outDegVeracity = Util.time("Out-Degree Veracity", OutDegreeVeracity(seed, synth))
      println(s"Out-Degree Veracity: $outDegVeracity")
    }

    if ( all || metrics.contains("out-degree") ) {
      val pageRankVeracity = Util.time("PageRank Veracity", PageRankVeracity(seed, synth))
      println(s"Page Rank Veracity: $pageRankVeracity")
    }
  }

  def run_synth(config: Config): Boolean = {
    var graphPs = null.asInstanceOf[GraphPersistence]
    config.backend match {
      case "fs" => graphPs = new SparkPersistence()
      case "neo4j" => graphPs = new Neo4jPersistence()
    }

    val seed = Util.time( "Load seed graph", {
      val seed = graphPs.loadGraph(config.seedGraphPrefix, config.partitions)
      println("Vertices #: " + seed.numVertices + ", Edges #: " + seed.numEdges)
      seed
    } )

    val seedDists = new DataDistributions(config.outLog)

    var synthesizer: GraphSynth = null
    config.synthesizer match {
      case "ba" => synthesizer = new ParallelBaSynth (config.partitions, config.iterations, config.sampleFraction)
      case "kro" => synthesizer = new KroSynth (config.partitions, config.seedMatrix, config.iterations)
    }

    val synth = synthesizer.synthesize(seed, seedDists, !config.skipProperties)

    Util.time( "Save synth graph Object", graphPs.saveGraph(synth, config.synthGraphPrefix, overwrite = true))

    if ( config.backend == "fs" ) {
      Util.time("Save synth graph Text", graphPs.asInstanceOf[SparkPersistence].saveAsText(synth, config.synthGraphPrefix + "_text", overwrite = true))
    }

    run_metrics(config.metrics, seed, synth)

    true
  }

  def run_veracity(config: Config): Boolean = {
    var graphPs = null.asInstanceOf[GraphPersistence]
    config.backend match {
      case "fs" => graphPs = new SparkPersistence()
      case "neo4j" => graphPs = new Neo4jPersistence()
    }

    val seed = graphPs.loadGraph(config.seedGraphPrefix, config.partitions)

    val synth = graphPs.loadGraph(config.synthGraphPrefix, config.partitions)

    run_metrics(config.metrics, seed, synth)

    true
  }

  def run_workload(params: Params): Boolean = {
    var graphPs = null.asInstanceOf[GraphPersistence]
    params.backend match {
      case "fs" => graphPs = new SparkPersistence()
      case "neo4j" => graphPs = new Neo4jPersistence()
    }

    val graph = Util.time( "Load graph", graphPs.loadGraph(params.graph, params.partitions) )

    Util.time( "Count vertices", SparkWorkload.countVertices(graph) )
    Util.time( "Count edges", SparkWorkload.countEdges(graph) )

    Util.time( "Neighbors", SparkWorkload.neighbors(graph) )
    Util.time( "In-neighbors", SparkWorkload.inNeighbors(graph) )
    Util.time( "Out-neighbors", SparkWorkload.outNeighbors(graph) )

    Util.time( "In-edges", SparkWorkload.inEdges(graph) )
    Util.time( "Out-edges", SparkWorkload.outEdges(graph) )

    Util.time( "Degree", SparkWorkload.degree(graph) )
    Util.time( "In-degree", SparkWorkload.inDegree(graph) )
    Util.time( "Out-degree", SparkWorkload.outDegree(graph) )

    Util.time( "Connected Components", SparkWorkload.connectedComponents(graph) )
    Util.time( "Strongly Connected Components", SparkWorkload.stronglyConnectedComponents(graph, 1) )
    Util.time( "PageRank", SparkWorkload.pageRank(graph) )
    Util.time( "Triangle Counting", SparkWorkload.triangleCount(graph) )
    Util.time( "Betweenness Centrality", SparkWorkload.betweennessCentrality(graph, 10) )
//    Util.time( "Closeness Centrality", SparkWorkload.closenessCentrality(vertex, graph) )
//    Util.time( "Breadth-first Search", SparkWorkload.bfs(graph, src, dst) )
//    Util.time( "Breadth-first Search", SparkWorkload.ssspSeq(graph, src, dst) )
//    Util.time( "Breadth-first Search", SparkWorkload.ssspNum(graph, src, dst) )

    true
  }
}
