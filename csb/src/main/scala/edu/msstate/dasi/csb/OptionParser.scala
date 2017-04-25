package edu.msstate.dasi.csb

import java.io.File

class OptionParser(override val programName: String, programVersion: String, config: Config) extends scopt.OptionParser[Config](programName) {
  head(s"$programName version $programVersion")

  version("version").hidden()

  opt[Int]('p', "partitions")
    .valueName("<n>")
    .text(s"The number of RDD partitions, default: the level of parallelism provided by Spark for this system [${config.partitions}]")
    .validate(value => if (value > 0) success else failure("partitions must be greater than 0"))
    .action((x, c) => c.copy(partitions = x))

  note("")

  cmd("seed")
    .text("")
    .action( (_, c) => c.copy(mode = "seed") )
    .children(
      arg[String]("bro_log")
        .optional()
        .text(s"The path of the Bro's connection log, default: ${config.connLog}")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(connLog = x)),

        arg[String]("alert_log")
        .optional()
        .text(s"The path of the Snort's connection log, default: ${config.alertLog}")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(alertLog = x)),

        arg[String]("aug_log")
        .optional()
        .text(s"The path where to save the resulting augmented log, default: ${config.augLog}")
        .action((x, c) => c.copy(connLog = x)),

        arg[String]("seed_graph")
        .optional()
        .text(s"The prefix used to save the generated seed graph, default: ${config.seedPrefix}")
        .action((x, c) => c.copy(connLog = x))
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

  def parse(args: Seq[String]): Option[Config] = super.parse(args, config)
}
