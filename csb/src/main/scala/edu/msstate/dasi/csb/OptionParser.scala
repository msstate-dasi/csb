package edu.msstate.dasi.csb

import java.io.File

class OptionParser(override val programName: String, programVersion: String, config: Config) extends scopt.OptionParser[Config](programName) {
  private val seedOutGraphPrefix = "seed"

  /**
   * Provides a wrapper method for the inheriting parse() method, avoiding the caller to specify again the default
   * Config instance.
   */
  def parse(args: Seq[String]): Option[Config] = super.parse(args, config)

  /*********************************************************************************************************************
   * Begin of the ordered sequence of builder methods ******************************************************************
   ********************************************************************************************************************/

  head(s"$programName version $programVersion")

  version("version").hidden()

  opt[Int]('p', "partitions")
    .valueName("<num>")
    .text(s"Number of RDD partitions [default (level of parallelism detected on this system): ${config.partitions}].")
    .validate(value => if (value > 0) success else failure("partitions must be greater than 0"))
    .action((x, c) => c.copy(partitions = x))

  note("")

  cmd("seed")
    .text("")
    .action( (_, c) => c.copy(mode = "seed", outGraphPrefix = seedOutGraphPrefix) )
    .children(
      opt[String]("alert-log").abbr("al")
        .valueName("<path>")
        .text(s"Path of the Snort connection log [default: ${config.alertLog}].")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(alertLog = x)),

      opt[String]("bro-log").abbr("bl")
        .valueName("<path>")
        .text(s"Path of the Bro connection log [default: ${config.connLog}].")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(connLog = x)),

      opt[String]("out-log").abbr("ol")
        .valueName("<path>")
        .text(s"Output path of the resulting augmented log [default: ${config.outLog}].")
        .action((x, c) => c.copy(connLog = x)),

      opt[String]("seed-graph").abbr("sg")
        .text(s"Output path prefix to save the generated seed graph [default: $seedOutGraphPrefix].")
        .action((x, c) => c.copy(outGraphPrefix = x))
    )

  note("")

  cmd("synth")
    .action( (_, c) => c.copy(mode = "synth") )
    .text("")

  note("")

  cmd("veracity")
    .action( (_, c) => c.copy(mode = "veracity") )
    .text("")

  note("")

  cmd("workload")
    .action( (_, c) => c.copy(mode = "workload") )
    .text("")

  checkConfig( c => if (c.mode == "") failure("command cannot be empty") else success )

  /*********************************************************************************************************************
   * End of the ordered sequence of builder methods ********************************************************************
   ********************************************************************************************************************/
}
