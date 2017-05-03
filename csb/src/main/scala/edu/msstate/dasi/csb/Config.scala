package edu.msstate.dasi.csb

import org.apache.spark.graphx.VertexId

case class Config(
                 debug: Boolean = false,
                 mode: String = "",
                 partitions: Int = sc.defaultParallelism,
                 backend: String = "fs",

                 seedGraphPrefix: String = "seed",
                 synthGraphPrefix: String = "synth",
                 iterations: Int = 0,

                 alertLog: String = "alert",
                 connLog: String = "conn.log",
                 augLog: String = "aug.log",

                 synthesizer: String = "",
                 skipProperties: Boolean = false,

                 sampleFraction: Double = 0.1,

                 seedMatrix: String = "seed.mtx",

                 metrics: Seq[String] = Seq("all"),

                 workloads: Seq[String] = Seq("all"),
                 workloadBackend: String = "spark",
                 graphPrefix: String = "graph",
                 srcVertex: VertexId = 0L,
                 dstVertex: VertexId = 0L,
                 patternPrefix: String = "pattern"
                 )
