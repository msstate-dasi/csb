package edu.msstate.dasi.csb

import org.apache.spark.graphx.VertexId

/**
 * Configuration object for the application. Contains all variables relevant to running methods. Not all will apply to
 * every application scenario.
 *
 * @param debug             Enables printing of extra command-line output and experimental code.
 * @param mode              Specifies the mode that the application should operate in (e.g. seed, synth, veracity).
 * @param partitions        Sets number of partitions to use for Spark RDD objects throughout the application.
 * @param seedGraphPrefix   Prefix to attach to seed graph when loading/saving it.
 * @param synthGraphPrefix  Prefix to attach to synthetic graph when loading/saving it.
 * @param seedDistributions Path to a serialized [[distributions.DataDistributions]] object file.
 * @param bucketSize        Size of byte-buckets to break distributions into.
 * @param graphLoader       Method to use when loading a graph into memory.
 * @param graphSaver        Backend to use when saving a graph.
 * @param textSaver         Backend to use when saving a graph as pure-text
 * @param iterations        Number of iterations to perform synthesis
 * @param connLog           Path to a Bro IDS connection log file, similar to a CSV file.
 * @param synthesizer       Method to use when synthesizing a new graph.
 * @param skipProperties    Defines whether the application skips over the property-generation sections.
 * @param sampleFraction    Percentage to sample during iterations of synthetic graph generation.
 * @param seedMatrix        Path to a Kronecker seed matrix file. Saved as an N by N matrix separated by tabs.
 * @param metrics           List of metrics to run on both seed and synthetic graphs when in veracity mode.
 * @param workloads         List of workloads to run on both seed and synthetic graphs when in workload mode.
 * @param workloadBackend   Computation backend for performing workloads.
 * @param neo4jUrl          URL string to neo4j instance if using neo4j backend.
 * @param neo4jUsername     Username to login to Neo4j with.
 * @param neo4jPassword     Password to login to Neo4j with.
 * @param graphPrefix       Path prefix of graph to load for workload mode.
 * @param srcVertex         Sets a single vertex to use as the source for workload mode.
 * @param dstVertex         Sets a single vertex to use as the destination for workload mode.
 * @param patternPrefix     Path prefix of the pattern graph to use for subgraph-isomorphism workload.
 */
case class Config(
                 debug: Boolean = false,
                 mode: String = "",
                 partitions: Int = sc.defaultParallelism,

                 seedGraphPrefix: String = "seed",
                 synthGraphPrefix: String = "synth",

                 seedDistributions: String = "seed.distributions",
                 bucketSize: Int = 10,

                 graphLoader: String = "spark",
                 graphSaver: String = "spark",
                 textSaver: String = "none",

                 iterations: Int = 0,

                 connLog: String = "conn.log",

                 synthesizer: String = "",
                 skipProperties: Boolean = false,

                 sampleFraction: Double = 0.1,

                 seedMatrix: String = "seed.mtx",

                 metrics: Seq[String] = Seq("all"),

                 workloads: Seq[String] = Seq("all"),
                 workloadBackend: String = "spark",

                 neo4jUrl: String = "bolt://localhost",
                 neo4jUsername: String = "neo4j",
                 neo4jPassword: String = "neo4j",

                 graphPrefix: String = "graph",
                 srcVertex: VertexId = 0L,
                 dstVertex: VertexId = 0L,
                 patternPrefix: String = "pattern"
                 )
