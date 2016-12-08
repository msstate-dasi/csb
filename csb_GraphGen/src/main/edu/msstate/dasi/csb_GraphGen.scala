package edu.msstate.dasi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable
import scala.reflect.runtime.universe._
import java.text.BreakIterator

/**
  * Created by spencer on 11/3/16.
  */

object csb_GraphGen{

  /**
    * Abstract class for parameter case classes.
    * This overrides the [[toString]] method to print all case class fields by name and value.
    * @tparam T  Concrete parameter class.
    */
  abstract class AbstractParams[T: TypeTag] {
    private def tag: TypeTag[T] = typeTag[T]
    /**
      * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
      * {
      *   [field name]:\t[field value]\n
      *   [field name]:\t[field value]\n
      *   ...
      * }
      */
    override def toString: String = {
      val tpe = tag.tpe
      val allAccessors = tpe.decls.collect {
        case m: MethodSymbol if m.isCaseAccessor => m
      }
      val mirror = runtimeMirror(getClass.getClassLoader)
      val instanceMirror = mirror.reflect(this)
      allAccessors.map { f =>
        val paramName = f.name.toString
        val fieldMirror = instanceMirror.reflectField(f)
        val paramValue = fieldMirror.get
        s"  $paramName:\t$paramValue"
      }.mkString("{\n", ",\n", "\n}")
    }
  }

  case class ParamsHelp(
                                 /**
                                   * Any Arguments
                                   */
                                 outputGraphPrefix_desc: String = "Prefix to use when saving the output graph",
                                 partitions_desc: String =  "Number of partitions to set RDDs to.",
                                 checkpointDir_desc: String = "Directory for checkpointing intermediate results. " +
                                   "Checkpointing helps with recovery and eliminates temporary shuffle files on disk",
                                 checkpointInterval_desc: String = "Iterations between each checkpoint.  Only used if " +
                                   "checkpointDir is set.",
                                 /**
                                   * GenDist Arguments
                                   */
                                 connLog_desc: String = "Bro IDS conn.log file to augment with a SNORT alert.log.",
                                 alertLog_desc: String = "SNORT alert augment with a Bro IDS conn.log.",
                                 augLog_desc: String = "Augmented Bro IDS conn.log and SNORT alert.log.",
                                 /**
                                   * BA Arguments
                                   */
                                 seedVertices_desc: String = "Comma-separated vertices file to use as a seed for BA Graph Generation.",
                                 seedEdges_desc: String = "Comma-separated edges file to use as a seed for BA Graph Generation.",
                                 JSONDist_desc: String = "JSON Distribution file to use for generating random properties.",
                                 baIter_desc: String = "Number of iterations for Barabasi–Albert model.",
                                 /**
                                   * Kronecker Arguments
                                   */
                                 seedMtx_desc: String = "Space-separated matrix file to use as a seed for Kronecker.",
                                 kroIter_desc: String = "Number of iterations for Kronecker model."

                               ) extends AbstractParams[ParamsHelp]

  case class Params(
                             mode: String = "",

                             /**
                               * Any Arguments
                               */
                             outputGraphPrefix: String = "out",
                             partitions: Int = 120,
                             checkpointDir: Option[String] = None,
                             checkpointInterval: Int = 10,
                             debug: String = "false",

                             /**
                               * GenDist Arguments
                               */
                             connLog: String = "conn.log",
                             alertLog: String = "alert",
                             augLog: String = "aug.log",
                             JSONDist: String = "dist.json",

                             /**
                               * BA Arguments
                               */
                             seedVertices: String = "seed_vert",
                             seedEdges: String = "seed_edges",
                             baIter: Int = 1000,

                             /**
                               * Kronecker Arguments
                               */
                             seedMtx: String = "seed.mtx",
                             kroIter: Int = 10

  ) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val dP = Params()
    val h = ParamsHelp()

    val parser = new OptionParser[Params]("csb_GraphGen") {
      head("csb_GraphGen: a synthetic Graph Generator for the busy scientist.")
      /**
        * All Arguments:
        */
      opt[String]("output")
        .text(s"${h.outputGraphPrefix_desc} default ${dP.outputGraphPrefix}")
        .action((x,c) => c.copy(outputGraphPrefix = x))
      opt[Int]("partitions")
        .text(s"${h.partitions_desc} default: ${dP.partitions}")
        .validate(x => if (x>0) success
          else failure("Partition count must be greater than 0."))
        .action((x,c) => c.copy(partitions = x))
      opt[String]("checkpointDir")
        .text(s"${h.checkpointDir_desc} default: ${dP.checkpointDir}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"${h.checkpointInterval_desc} default: ${dP.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      opt[Unit]("debug")
        .hidden()
        .action( (x, c) => c.copy(debug = "true"))
        .text(s"Debug mode, prints all log output to terminal. default: ${dP.debug}")

      /**
        * GenDist Arguments:
        */
      note("\n")
      cmd("gen_dist").action((_,c) => c.copy(mode = "gen_dist"))
        .text(s"Generate distribution data for a given input dataset.")
        .children(
          arg[String]("bro_log")
            .text(s"${h.connLog_desc} default: ${dP.connLog}")
            .action((x,c) => c.copy(connLog = x)),
          arg[String]("alert_log")
            .text(s"${h.alertLog_desc} default: ${dP.alertLog}")
            .action((x,c) => c.copy(alertLog = x)),
          arg[String]("aug_log")
            .text(s"${h.augLog_desc} default: ${dP.augLog}")
            .action((x,c) => c.copy(augLog = x)),
          arg[String]("dist_out")
            .text(s"Path to save ${h.JSONDist_desc} default: ${dP.JSONDist}")
            .action((x,c) => c.copy(JSONDist = x))
        )

      /**
        * BA Arguments:
        */
      note("\n")
      cmd("ba").action((_, c) => c.copy(mode = "ba"))
        .text(s"Generate synthetic graph using the Barabasi–Albert model.")
        .children(
          arg[String]("seed_vert")
            .text(s"${h.seedVertices_desc} default: ${dP.seedVertices}")
            .required()
            .action((x,c) => c.copy(seedVertices = x)),
          arg[String]("seed_edges")
            .text(s"${h.seedEdges_desc} default: ${dP.seedEdges}")
            .required()
            .action((x,c) => c.copy(seedEdges = x)),
          arg[String]("dist")
            .text(s"${h.JSONDist_desc} default: ${dP.JSONDist}")
            .required()
            .action((x,c) => c.copy(JSONDist = x)),
          arg[Int]("<# of Iterations>")
            .text(s"${h.baIter_desc} default: ${dP.baIter}")
            .validate(x => if (x>0) success
              else failure("Iteration count must be greater than 0."))
            .action((x,c) => c.copy(baIter = x))
        )

      /**
        * Kronecker Arguments
        */
      note("\n")
      cmd("kro").action((_, c) => c.copy(mode = "kro"))
        .text(s"Generate synthetic graph using the Probabilistic Kronecker model.")
        .children(
          arg[String]("seed-mtx")
            .text(s"${h.seedMtx_desc} default: ${dP.seedMtx}")
            .required()
            .action((x,c) => c.copy(seedMtx = x)),
          arg[String]("dist")
            .text(s"${h.JSONDist_desc} default: ${dP.JSONDist}")
            .required()
            .action((x,c) => c.copy(JSONDist = x)),
          arg[Int]("<# of Iterations>")
            .text(s"${h.kroIter_desc} default: ${dP.baIter}")
            .validate(x => if (x>0) success
            else failure("Iteration count must be greater than 0."))
            .action((x,c) => c.copy(baIter = x))
        )
      note("\n")

    }

    parser.parse(args, dP) match {
      case Some(params) => if (params.mode != "") run(params) else {println("Error: Must specify command"); parser.showUsageAsError()}
      case _ => sys.exit(1)
    }
  }

  /*** Main function of our program, controls graph generation and other pieces.
    *
    * @param params Parameters for the function to run.
    */
  def run(params: Params): Boolean = {
    if (params.debug == "false") {
      //turn off annoying log messages
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    } else {

    }
    //every spark application needs a configuration and sparkcontext
    val conf = new SparkConf().setAppName("csb_GraphGen")
    val sc = new SparkContext(conf)


    params.mode match {
      case "gen_dist" => println("gen_dist")
      case "ba" => run_ba(sc, params)
      case "kro" => println("kro")
      case _ => sys.exit(1)
    }

    return true
  }

  def run_gendist(sc: SparkContext, params: Params): Boolean = {
    val distParser: multiEdgeDistribution = new multiEdgeDistribution()
    distParser.init(Array(params.augLog))

    return true
  }
  def run_ba(sc: SparkContext, params: Params): Boolean = {
    val baGraph = new ba_GraphGen()
    baGraph.run(sc, params.seedVertices, params.seedEdges, params.baIter)

    return true

  }
  def run_kro(sc: SparkContext, params: Params): Boolean = {
    val kroGraph = new kro_GraphGen()
    kroGraph.run(sc, params.seedMtx, params.kroIter)

    return true
  }




}