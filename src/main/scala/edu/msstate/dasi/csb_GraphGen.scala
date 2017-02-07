package edu.msstate.dasi

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scopt.OptionParser
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by spencer on 11/3/16.
  */

object csb_GraphGen extends DataParser {

  val versionString = "0.2-DEV"

  val neo4jBoltHost     = "localhost"
  val neo4jBoltPort     = "7687"
  val neo4jBoltUser     = "neo4j"
  val neo4jBoltPassword = "stefano"
  val neo4jBoltUrl: String =
    "bolt://" + neo4jBoltUser + ":" + neo4jBoltPassword + "@" + neo4jBoltHost + ":" + neo4jBoltPort

  case class ParamsHelp(
                         /**
                           * Any Arguments
                           */
                         outputGraphPrefix_desc: String = "Prefix to use when saving the output graph",
                         partitions_desc: String = "Number of partitions to set RDDs to.",
                         backend_desc: String = "Backend used to save generated data (fs or neo4j).",
                         checkpointDir_desc: String = "Directory for checkpointing intermediate results. " +
                           "Checkpointing helps with recovery and eliminates temporary shuffle files on disk.",
                         checkpointInterval_desc: String = "Iterations between each checkpoint. Only used if " +
                           "checkpointDir is set.",

                         /**
                           * GenDist Arguments
                           */
                         connLog_desc: String = "Bro IDS conn.log file.",
                         alertLog_desc: String = "SNORT alert augment.",
                         augLog_desc: String = "Augmented Bro IDS conn.log and SNORT alert.log.",

                         /**
                           * BA Arguments
                           */
                         noProp_desc: String = "Specify whether to generate random properties during generation or not.",
                         numNodesPerIter_desc: String = "The number of nodes to add to the graph per iteration.",
                         seedVertices_desc: String = "Comma-separated vertices file to use as a seed for BA Graph Generation.",
                         seedEdges_desc: String = "Comma-separated edges file to use as a seed for BA Graph Generation.",
                         baIter_desc: String = "Number of iterations for Barabasi–Albert model.",

                         /**
                           * Kronecker Arguments
                           */
                         seedMtx_desc: String = "Space-separated matrix file to use as a seed for Kronecker.",
                         kroIter_desc: String = "Number of iterations for Kronecker model.",

                         /**
                           * KronFit Arguements
                           */
                       mtx_desc: String = "Initial matrix in the form of mtx[0],mtx[1],mtx[2],mtx[3] (NO SPACES BETWEEN COMMAS) where mtx[x] is a real value between 1 and 0.",


                         /**
                           * veracity arguements
                           */
                       veracity_Desc: String = "The veracity metric you want to compute. Options include: degree, inDegree, outDegree, pageRank",
                         veracity_File: String = "The file to save the metric information",
                         seed_vertsMetric: String = "Comma-separated vertices file to use as a seed",
                         seed_edgeMetric: String = "Comma-separated edges file to use as a seed",
                         synth_vertsMetric: String = "Comma-seperated generated vertices file to test",
                         synth_edgeMetric: String = "Comma-seperated generated edge file to test"

                       )

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
                     numNodesPerIter: Long = 120,
                     seedVertices: String = "seed_vert",
                     seedEdges: String = "seed_edges",
                     baIter: Long = 1000,

                     /**
                       * Kronecker Arguments
                       */
                     seedMtx: String = "seed.mtx",
                     kroIter: Int = 10,

                     /**
                       * Kronfit Arguments
                       */
                   initialMtx: String = "0.9,0.1,0.5,0.3",

                     /**
                       * veracity arguements
                       */
                     metric: String = "hop-plot",
                     metricSave: String = "hop-plotSave",
                     synthVerts: String = "syth_verts",
                     synthEdges: String = "syth_edges"

                   )

  def main(args: Array[String]) {
    val dP = Params()
    val h = ParamsHelp()

    val parser = new OptionParser[Params]("csb_GraphGen") {
      head(s"csb_GraphGen $versionString: a synthetic Graph Generator for the busy scientist.")

      /**
        * All Arguments:
        */
      opt[String]("output")
        .text(s"${h.outputGraphPrefix_desc} default ${dP.outputGraphPrefix}")
        .action((x, c) => c.copy(outputGraphPrefix = x))
      opt[Int]("partitions")
        .text(s"${h.partitions_desc} default: ${dP.partitions}")
        .validate(x => if (x > 0) success else failure("Partition count must be greater than 0."))
        .action((x, c) => c.copy(partitions = x))
      opt[String]("backend")
        .text(s"${h.backend_desc} default: ${dP.backend}")
        .validate(x => if (x == "fs" || x == "neo4j") success else failure("Backend must be fs or neo4j."))
        .action((x, c) => c.copy(backend = x))
      opt[String]("checkpointDir")
        .text(s"${h.checkpointDir_desc} default: ${dP.checkpointDir}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"${h.checkpointInterval_desc} default: ${dP.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      opt[Unit]("debug")
        .hidden()
        .action((_, c) => c.copy(debug = true))
        .text(s"Debug mode, prints all log output to terminal. default: ${dP.debug}")

      /**
        * GenDist Arguments:
        */
      note("\n")
      cmd("gen_dist").action((_, c) => c.copy(mode = "gen_dist"))
        .text(s"Generate distribution data for a given input dataset.")
        .children(
          arg[String]("bro_log")
            .text(s"${h.connLog_desc} default: ${dP.connLog}")
            .required()
            .action((x, c) => c.copy(connLog = x)),
          arg[String]("alert_log")
            .text(s"${h.alertLog_desc} default: ${dP.alertLog}")
            .required()
            .action((x, c) => c.copy(alertLog = x)),
          arg[String]("aug_log")
            .text(s"${h.augLog_desc} default: ${dP.augLog}")
            .required()
            .action((x, c) => c.copy(augLog = x)),
          arg[String]("seed_vert")
            .text(s"Output file for ${h.seedVertices_desc} default: ${dP.seedVertices}")
            .required()
            .action((x, c) => c.copy(seedVertices = x)),
          arg[String]("seed_edges")
            .text(s"Output file for ${h.seedEdges_desc} default: ${dP.seedEdges}")
            .required()
            .action((x, c) => c.copy(seedEdges = x))
        )

      /**
        * BA Arguments:
        */
      note("\n")
      cmd("ba").action((_, c) => c.copy(mode = "ba"))
        .text(s"Generate synthetic graph using the Barabasi–Albert model.")
        .children(
          opt[Unit]("no-prop")
            .text(s"${h.noProp_desc} default: ${dP.noProp}")
            .action((_, c) => c.copy(noProp = true)),
          opt[Long]("nodes-per-iter")
            .text(s"${h.numNodesPerIter_desc} default: ${dP.numNodesPerIter}")
            .action((x, c) => c.copy(numNodesPerIter = x)),
          arg[String]("seed_vert")
            .text(s"${h.seedVertices_desc} default: ${dP.seedVertices}")
            .required()
            .action((x, c) => c.copy(seedVertices = x)),
          arg[String]("seed_edges")
            .text(s"${h.seedEdges_desc} default: ${dP.seedEdges}")
            .required()
            .action((x, c) => c.copy(seedEdges = x)),
          arg[Int]("<# of Iterations>")
            .text(s"${h.baIter_desc} default: ${dP.baIter}")
            .validate(x => if (x > 0) success else failure("Iteration count must be greater than 0."))
            .action((x, c) => c.copy(baIter = x))
        )

      /**
        * Kronecker Arguments
        */
      note("\n")
      cmd("kro").action((_, c) => c.copy(mode = "kro"))
        .text(s"Generate synthetic graph using the Probabilistic Kronecker model.")
        .children(
          opt[Unit]("no-prop")
            .text(s"${h.noProp_desc} default ${dP.noProp}")
            .action((_, c) => c.copy(noProp = true)),
          arg[String]("seed-mtx")
            .text(s"${h.seedMtx_desc} default: ${dP.seedMtx}")
            .required()
            .action((x, c) => c.copy(seedMtx = x)),
          arg[String]("seed_vert")
            .text(s"${h.seedVertices_desc} default: ${dP.seedVertices}")
            .required()
            .action((x, c) => c.copy(seedVertices = x)),
          arg[String]("seed_edges")
            .text(s"${h.seedEdges_desc} default: ${dP.seedEdges}")
            .required()
            .action((x, c) => c.copy(seedEdges = x)),
          arg[Int]("<# of Iterations>")
            .text(s"${h.kroIter_desc} default: ${dP.kroIter}")
            .validate(x => if (x > 0) success else failure("Iteration count must be greater than 0."))
            .action((x, c) => c.copy(kroIter = x))
        )
      note("\n")
      cmd("krofit").action((_, c) => c.copy(mode = "krofit"))
        .text(s"Compute the inital matrix given a graph so that it will be possible to use kronecker")
        .children(
          arg[String]("bro_log")
            .text(s"${h.connLog_desc} default: ${dP.connLog}")
            .required()
            .action((x, c) => c.copy(connLog = x)),
          arg[String]("inital_matrix")
            .text(s"${h.mtx_desc}")
            .required()
            .action((x, c) => c.copy(initialMtx = x))
        )
      note("\n")

      cmd("ver").action((_, c) => c.copy(mode = "ver"))
        .text(s"Compute veracity metrics on a given vertices and edge seed files")
        .children(
          arg[String]("seed_vert")
              .text(s"${h.seed_vertsMetric} default: ${dP.seedVertices}")
              .required()
              .action((x,c) => c.copy(seedVertices = x)),
          arg[String]("seed_edges")
              .text(s"${h.seed_edgeMetric} default: ${dP.seedEdges}")
              .required()
              .action((x, c) => c.copy(seedEdges = x)),
          arg[String]("synth_vert")
              .text(s"${h.synth_vertsMetric}")
              .required()
              .action((x,c) => c.copy(synthVerts = x)),
          arg[String]("syth_edge")
              .text(s"${h.synth_edgeMetric}")
              .required()
              .action((x,c) => c.copy(synthEdges = x)),
          arg[String]("metric")
            .text(s"${h.veracity_Desc}")
            .required()
            .action((x,c) => c.copy(metric = x)),
          arg[String]("save_file")
            .text(s"${h.veracity_File}")
            .required()
            .action((x,c) => c.copy(metricSave = x))
        )

    }

    parser.parse(args, dP) match {
      case Some(params) => if (params.mode != "") run(params)
      else {
        println("Error: Must specify command")
        parser.showUsageAsError()
      }
      case _ => sys.exit(1)
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


    // Create a SparkSession. No need to create SparkContext
    // You automatically get it as part of the SparkSession
    val warehouseLocation = "spark-warehouse"
    val spark = SparkSession
      .builder()
      .appName("Cyber Security Benchmark")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.neo4j.bolt.url", neo4jBoltUrl)
      .getOrCreate()
    val sc = spark.sparkContext


    params.mode match {
      case "gen_dist" => run_gendist(sc, params)
      case "ba" => run_ba(sc, params)
      case "kro" => run_kro(sc, params)
      case "krofit" => run_krofit(sc, params)
      case "ver" => run_ver(sc, params)
      case _ => sys.exit(1)
    }

    sys.exit()
    true
  }


  def run_gendist(sc: SparkContext, params: Params): Boolean = {
    //these two statements create the aug log (conn.log plus alert)
    val logAug = new log_Augment()
    logAug.run(sc, params.alertLog, params.connLog, params.augLog)

  //this is called to read the newly created aug log and create the distributions from it
    new DataDistributions(sc, params.augLog)

    val (vRDD, eRDD): (RDD[(VertexId, nodeData)], RDD[Edge[edgeData]]) = readFromConnFile(sc, params.partitions, params.augLog)

    val theGraph = Graph(vRDD, eRDD, nodeData())


    val seed_vert = theGraph.vertices.coalesce(1, true).collect()
    val seed_vert_file = new File(params.seedVertices)

    var bw = new BufferedWriter(new FileWriter(seed_vert_file))
//    bw.write("ID,Desc\n")
    for (entry <- seed_vert) {
      bw.write(entry._1 + "," + entry._2 + "\n")
    }
    bw.flush()

    val seed_edges = theGraph.edges.coalesce(1, true).collect()
    val seed_edge_file = new File(params.seedEdges)

    bw = new BufferedWriter(new FileWriter(seed_edge_file))

    for (entry <- seed_edges) {
      bw.write(entry.srcId + "," + entry.dstId + "," + entry.attr + "\n")
    }
    bw.close()

    true
  }

  def run_ba(sc: SparkContext, params: Params): Boolean = {
    var graphPs: GraphPersistence = null
    params.backend match {
      case "fs" => graphPs = new SparkPersistence(params.outputGraphPrefix)
      case "neo4j" => graphPs = new Neo4jPersistence(sc)
    }

    val baGraph = new BaSynth(sc, params.partitions, new DataDistributions(sc, params.augLog), graphPs, params.baIter, params.numNodesPerIter)
    baGraph.run(params.seedVertices, params.seedEdges, params.noProp)

    true
  }

  def run_kro(sc: SparkContext, params: Params): Boolean = {
    var graphPs: GraphPersistence = null
    params.backend match {
      case "fs" => graphPs = new SparkPersistence(params.outputGraphPrefix)
      case "neo4j" => graphPs = new Neo4jPersistence(sc)
    }

    val kroGraph = new KroSynth(sc, params.partitions, new DataDistributions(sc, params.augLog), graphPs, params.seedMtx, params.kroIter)
    kroGraph.run(params.seedVertices, params.seedEdges, params.noProp)

    true
  }

  def run_krofit(sc: SparkContext, params: Params): Boolean =
  {
    val krofit = new KroFit(sc, params.partitions, params.initialMtx, 50, "graph.txt")
    krofit.run()

    true
  }

  def run_ver(sc: SparkContext, params: Params): Boolean = {
    val (seedVerts, seedEdges) = readFromSeedGraph(sc, params.partitions, params.seedVertices, params.seedEdges)
    val seed = Graph(seedVerts, seedEdges, nodeData())

    val (synthVerts, sythEdges) = readFromSeedGraph(sc, params.partitions, params.synthVerts, params.synthEdges)
    val synth = Graph(synthVerts, sythEdges, nodeData())

    params.metric match {
      case "degree" =>
        val startTime = System.nanoTime()
        val degree = Degree(seed, synth, saveDistAsCSV = true, overwrite = true)
        val timeSpan = (System.nanoTime() - startTime) / 1e9
        println(s"\tPage Rank Veracity: $degree [$timeSpan s]")

      case "inDegree" =>
        val startTime = System.nanoTime()
        val inDegree = InDegree(seed, synth, saveDistAsCSV = true, overwrite = true)
        val timeSpan = (System.nanoTime() - startTime) / 1e9
        println(s"\tPage Rank Veracity: $inDegree [$timeSpan s]")

      case "outDegree" =>
        val startTime = System.nanoTime()
        val outDegree = OutDegree(seed, synth, saveDistAsCSV = true, overwrite = true)
        val timeSpan = (System.nanoTime() - startTime) / 1e9
        println(s"\tPage Rank Veracity: $outDegree [$timeSpan s]")

      case "pageRank" =>
        val startTime = System.nanoTime()
        val pageRank = PageRank(seed, synth, saveDistAsCSV = true, overwrite = true)
        val timeSpan = (System.nanoTime() - startTime) / 1e9
        println(s"\tPage Rank Veracity: $pageRank [$timeSpan s]")

      case _ => println("Invalid metric:" + params.metric)
    }

    true
  }
}
