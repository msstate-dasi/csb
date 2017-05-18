package edu.msstate.dasi.csb

import java.io.File

class OptionParser(override val programName: String, programVersion: String, config: Config) extends scopt.OptionParser[Config](programName) {
  /**
   * Provides a wrapper method for the inheriting parse() method, avoiding the caller to specify again the default
   * Config instance.
   */
  def parse(args: Seq[String]): Option[Config] = super.parse(args, config)

  override def showUsageOnError = true

  /*********************************************************************************************************************
   * Begin of the ordered builder methods ******************************************************************************
   ********************************************************************************************************************/

  head(s"$programName version $programVersion")

  version("version").hidden()

  help("help").hidden()

  opt[Int]('p', "partitions")
    .valueName("<num>")
    .text(s"Number of RDD partitions [default detected level of parallelism: ${config.partitions}].")
    .validate(value => if (value > 0) success else failure("partitions must be greater than 0"))
    .action((x, c) => c.copy(partitions = x))

  opt[Unit]("debug")
    .hidden()
    .action((_, c) => c.copy(debug = true))

  note("")

  cmd("seed")
    .text("Generates a property graph and the probability distributions of its properties starting from two log files.")
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
        .text(s"Output path of the resulting augmented log [default: ${config.augLog}].")
        .action((x, c) => c.copy(augLog = x)),

      opt[String]('w', "saver")
        .valueName("<value>")
        .text(s"Graph saver. Supported: spark|neo4j|none [default: ${config.graphSaver}].")
        .action((x, c) => c.copy(graphSaver = x)),

      opt[String]('o', "out-graph")
        .valueName("<path>")
        .text(s"Output path prefix of the generated seed graph [default: ${config.seedGraphPrefix}].")
        .action((x, c) => c.copy(seedGraphPrefix = x)),

        opt[String]('t', "as-text")
        .valueName("<format>")
        .text(s"Graph saver for text format. Supported: spark|neo4j|none [default: ${config.textSaver}].")
        .action((x, c) => c.copy(textSaver = x)),

      opt[String]('d', "distributions")
        .valueName("<path>")
        .text(s"Output path of the generated seed property distributions [default: ${config.seedDistributions}].")
        .action((x, c) => c.copy(seedGraphPrefix = x))
    )

  note("")

  cmd("synth")
    .text("Synthesizes a graph starting from a seed graph and the probability distributions of its properties.")
    .action( (_, c) => c.copy(mode = "synth", metrics = Seq()) )
    .children(
      opt[String]('r', "loader")
        .valueName("<value>")
        .text(s"Graph loader. Supported: spark|neo4j [default: ${config.graphLoader}].")
        .action((x, c) => c.copy(graphLoader = x)),

      opt[String]('s', "seed-graph")
        .valueName("<path>")
        .text(s"Path prefix of the seed graph [default: ${config.seedGraphPrefix}].")
        .action((x, c) => c.copy(seedGraphPrefix = x)),

      opt[String]('l', "log")
        .valueName("<path>")
        .text(s"Path of the augmented log [default: ${config.augLog}].")
        .validate(path => if ( new File(path).isFile ) success else failure(s"$path is not a regular file") )
        .action((x, c) => c.copy(augLog = x)),

      opt[String]('w', "saver")
        .valueName("<value>")
        .text(s"Graph saver. Supported: spark|neo4j|none [default: ${config.graphSaver}].")
        .action((x, c) => c.copy(graphSaver = x)),

      opt[String]('o', "out-graph")
        .valueName("<path>")
        .text(s"Output path prefix of the generated synthetic graph [default: ${config.synthGraphPrefix}].")
        .action((x, c) => c.copy(synthGraphPrefix = x)),

      opt[String]('t', "as-text")
        .valueName("<format>")
        .text(s"Graph saver for text format. Supported: spark|neo4j|none [default: ${config.textSaver}].")
        .action((x, c) => c.copy(textSaver = x)),

      opt[String]('d', "distributions")
        .valueName("<path>")
        .text(s"Path of the seed property distributions [default: ${config.seedDistributions}].")
        .action((x, c) => c.copy(seedGraphPrefix = x)),

      opt[Unit]('x', "exclude-properties")
        .text(s"Skip the properties generation [default: ${config.skipProperties}].")
        .action((_, c) => c.copy(skipProperties = true)),

      opt[Seq[String]]('m', "metrics")
        .valueName("<metric1,metric2,...>")
        .text(s"Comma separated list of veracity metrics to execute. Available: " +
          s"degree|in-degree|out-degree|pagerank|all [default:].")
        .action( (x, c) => c.copy(metrics = x) ),

      note(""),

      cmd("ba")
        .text("Synthesizes the graph using the Barabási–Albert algorithm.")
        .action( (_, c) => c.copy(synthesizer = "ba") )
        .children(
          arg[Int]("iterations")
            .valueName("<num>")
            .text(s"Number of algorithm's iterations.")
            .validate(value => if (value > 0) success else failure("iterations must be greater than 0"))
            .action((x, c) => c.copy(iterations = x)),

          arg[Double]("fraction")
            .optional()
            .text(s"Fraction of the data extracted at each iteration [default: ${config.sampleFraction}].")
            .action((x, c) => c.copy(sampleFraction = x))
        ),

      note(""),

      cmd("kro")
        .text("Synthesizes the graph using the Kronecker algorithm.")
        .action( (_, c) => c.copy(synthesizer = "kro") )
        .children(
          arg[Int]("iterations")
            .valueName("<num>")
            .text(s"Number of algorithm's iterations.")
            .validate(value => if (value > 0) success else failure("iterations must be greater than 0"))
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
    .text("Executes one or more veracity metrics. Available metrics: degree|in-degree|out-degree|pagerank|all")
    .action( (_, c) => c.copy(mode = "veracity") )
    .children(
      opt[String]('r', "loader")
        .valueName("<value>")
        .text(s"Graph loader. Supported: spark|neo4j [default: ${config.graphLoader}].")
        .action((x, c) => c.copy(graphLoader = x)),

      arg[Seq[String]]("<metric1,metric2,...>")
        .text(s"Comma separated list of veracity metrics to execute [default: ${config.metrics.mkString(",")}].")
        .action( (x, c) => c.copy(metrics = x) ),

      arg[String]("seed")
        .optional()
        .valueName("<path>")
        .text(s"Path prefix of the seed graph [default: ${config.seedGraphPrefix}].")
        .action((x, c) => c.copy(seedGraphPrefix = x)),

      arg[String]("synth")
        .optional()
        .valueName("<path>")
        .text(s"Path prefix of the synth graph [default: ${config.synthGraphPrefix}].")
        .action((x, c) => c.copy(synthGraphPrefix = x))
    )

  note("")

  cmd("workload")
    .text("Executes one or more workloads. Available workloads: count-vertices|count-edges|degree|in-degree|out-degree|" +
      "pagerank|bfs|neighbors|in-neighbors|out-neighbors|in-edges|out-edges|connected-components|triangle-count|" +
      "strongly-connected-components|betweenness-centrality|closeness-centrality|sssp|subgraph-isomorphism|all ")
    .action( (_, c) => c.copy(mode = "workload") )
    .children(
      opt[String]('b', "backend")
        .text(s"Workload backend. Supported: spark|neo4j [default: ${config.workloadBackend}].")
        .validate(value => if (value == "spark" || value == "neo4j") success else failure("workload backend not supported"))
        .action( (x, c) => c.copy(workloadBackend = x) ),

      opt[String]("neo4j-url")
        .text(s"Neo4j connection URL [default: ${config.neo4jUrl}].")
        .action( (x, c) => c.copy(neo4jUrl = x) ),

      opt[String]("neo4j-username")
        .text(s"Neo4j username [default: ${config.neo4jUsername}].")
        .action( (x, c) => c.copy(neo4jUsername = x) ),

      opt[String]("neo4j-password")
        .text(s"Neo4j password [default: ${config.neo4jPassword}].")
        .action( (x, c) => c.copy(neo4jPassword = x) ),

      opt[Int]('i', "iterations")
        .valueName("<num>")
        .text(s"Number of algorithm's iterations. " +
          s"Required by: strongly-connected-components|betweenness-centrality [default:].")
        .validate(value => if (value > 0) success else failure("iterations must be greater than 0"))
        .action( (x, c) => c.copy(iterations = x) ),

      opt[Long]('s', "source")
        .valueName("<id>")
        .text(s"The source vertex ID. " +
          s"Required by: bfs|closeness-centrality|sssp [default: random].")
        .validate(value => if (value > 0) success else failure("source must be greater than 0"))
        .action( (x, c) => c.copy(srcVertex= x) ),

      opt[Long]('d', "destination")
        .valueName("<id>")
        .text(s"The destination vertex ID. " +
          s"Required by: bfs [default: random].")
        .validate(value => if (value > 0) success else failure("destination must be greater than 0"))
        .action( (x, c) => c.copy(dstVertex = x) ),

      opt[String]('r', "loader")
        .valueName("<value>")
        .text(s"Graph loader. Supported: spark|neo4j [default: ${config.graphLoader}].")
        .action((x, c) => c.copy(graphLoader = x)),

      opt[String]('t', "pattern")
        .valueName("<path>")
        .text(s"Path prefix of the pattern graph. Required by: subgraph-isomorphism [default: ${config.patternPrefix}].")
        .validate(value => if (value != "") success else failure("pattern cannot be empty"))
        .action( (x, c) => c.copy(patternPrefix = x) ),

      opt[String]('g', "graph")
        .valueName("<path>")
        .text(s"Path prefix of the graph [default: ${config.graphPrefix}].")
        .action((x, c) => c.copy(graphPrefix = x)),

      arg[Seq[String]]("<workload1,workload2,...>")
        .optional()
        .text(s"Comma separated list of workloads to execute [default: ${config.workloads.mkString(",")}].")
        .action( (x, c) => c.copy(workloads = x) )
    )

  checkConfig( c => if (c.mode == "") failure("command cannot be empty") else success )

  checkConfig( c => {
    val iterationWorkloads = Seq("strongly-connected-components", "betweenness-centrality")
    if (c.workloads.intersect(iterationWorkloads).nonEmpty && c.iterations == 0) {
      failure("workload iterations not specified")
    } else {
      success
    }
  })

  /*********************************************************************************************************************
   * End of the ordered builder methods ********************************************************************************
   ********************************************************************************************************************/
}
