package edu.msstate.dasi.csb

import java.io.File

class OptionParser(override val programName: String, programVersion: String, config: Config) extends scopt.OptionParser[Config](programName) {
  /**
   * Provides a wrapper method for the inheriting parse() method, avoiding the caller to specify again the default
   * Config instance.
   */
  def parse(args: Seq[String]): Option[Config] = super.parse(args, config)

  /*********************************************************************************************************************
   * Begin of the ordered builder methods ******************************************************************************
   ********************************************************************************************************************/

  head(s"$programName version $programVersion")

  version("version").hidden()

  opt[Int]('p', "partitions")
    .valueName("<num>")
    .text(s"Number of RDD partitions [default level of parallelism detected on this system: ${config.partitions}].")
    .validate(value => if (value > 0) success else failure("partitions must be greater than 0"))
    .action((x, c) => c.copy(partitions = x))

  note("")

  cmd("seed")
    .text("")
    .action( (_, c) => c.copy(mode = "seed") )
    .children(
      opt[String]('a', "alert-log")
        .valueName("<path>")
        .text(s"Path of the Snort connection log [default: ${config.alertLog}].")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(alertLog = x)),

      opt[String]('b', "bro-log")
        .valueName("<path>")
        .text(s"Path of the Bro connection log [default: ${config.connLog}].")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(connLog = x)),

      opt[String]('l', "log")
        .valueName("<path>")
        .text(s"Output path of the resulting augmented log [default: ${config.outLog}].")
        .action((x, c) => c.copy(connLog = x)),

      opt[String]('o', "out-graph")
        .valueName("<path>")
        .text(s"Output path prefix of the generated seed graph [default: ${config.seedGraphPrefix}].")
        .action((x, c) => c.copy(seedGraphPrefix = x))
    )

  note("")

  cmd("synth")
    .text("")
    .action( (_, c) => c.copy(mode = "synth", metrics = Seq()) )
    .children(
      opt[String]('s', "seed-graph")
        .valueName("<path>")
        .text(s"Path prefix of the seed graph [default: ${config.seedGraphPrefix}].")
        .action((x, c) => c.copy(seedGraphPrefix = x)),

      opt[String]('o', "out-graph")
        .valueName("<path>")
        .text(s"Output path prefix of the generated synthetic graph [default: ${config.synthGraphPrefix}].")
        .action((x, c) => c.copy(synthGraphPrefix = x)),

      opt[Unit]('x', "exclude-prop")
        .text(s"Skip the properties generation [default: ${config.skipProperties}].")
        .action((_, c) => c.copy(skipProperties = true)),

      opt[Seq[String]]('m', "metrics")
        .valueName("<list>")
        .text(s"Comma separated list of veracity metrics to execute. Available: degree|in-degree|out-degree|pageRank|all [default: none].")
        .action( (x, c) => c.copy(metrics = x) ),

      note(""),

      cmd("ba")
        .text("")
        .action( (_, c) => c.copy(synthesizer = "ba") )
        .children(
          arg[Int]("iterations")
            .text(s"Number of algorithm's iterations.")
            .action((x, c) => c.copy(iterations = x)),

          arg[Double]("fraction")
            .optional()
            .text(s"Fraction of the data extracted at each iteration [default: ${config.sampleFraction}].")
            .action((x, c) => c.copy(sampleFraction = x))
        ),

      note(""),

      cmd("kro")
        .text("")
        .action( (_, c) => c.copy(synthesizer = "kro") )
        .children(
          arg[Int]("iterations")
            .text(s"Number of algorithm's iterations.")
            .action((x, c) => c.copy(iterations = x)),

          arg[String]("matrix")
            .optional()
            .valueName("<path>")
            .text(s"Path of the matrix file representing the starting seed [default: ${config.seedMatrix}].")
            .action((x, c) => c.copy(seedMatrix = x))
      )
    )

  note("")

  cmd("veracity")
    .text("")
    .action( (_, c) => c.copy(mode = "veracity") )
    .children(
      arg[Seq[String]]("metrics")
        .text(s"Comma separated list of veracity metrics to execute. Available: degree|in-degree|out-degree|pageRank|all [default: ${config.metrics.mkString(",")}].")
        .action( (x, c) => c.copy(metrics = x) ),

      arg[String]("seed")
        .optional()
        .text(s"Path prefix of the seed graph [default: ${config.seedGraphPrefix}].")
        .action((x, c) => c.copy(seedGraphPrefix = x)),

      arg[String]("synth")
        .optional()
        .text(s"Path prefix of the seed graph [default: ${config.synthGraphPrefix}].")
        .action((x, c) => c.copy(synthGraphPrefix = x))
    )

  note("")

  cmd("workload")
    .text("")
    .action( (_, c) => c.copy(mode = "workload") )

  checkConfig( c => if (c.mode == "") failure("command cannot be empty") else success )

  /*********************************************************************************************************************
   * End of the ordered builder methods ********************************************************************************
   ********************************************************************************************************************/
}
