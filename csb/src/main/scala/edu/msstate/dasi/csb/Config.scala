package edu.msstate.dasi.csb

import org.apache.spark.graphx.VertexId

case class Config(
                 debug: Boolean = false,
                 mode: String = "",
                 partitions: Int = sc.defaultParallelism,

                 seedGraphPrefix: String = "seed",
                 synthGraphPrefix: String = "synth",
                 graphLoader: String = "spark",
                 graphSaver: String = "spark",
                 textSaver: String = "none",
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

                 neo4jUrl: String = "bolt://localhost",
                 neo4jUsername: String = "neo4j",
                 neo4jPassword: String = "neo4j",

                 graphPrefix: String = "graph",
                 srcVertex: VertexId = 0L,
                 dstVertex: VertexId = 0L,
                 patternPrefix: String = "pattern"
                 )
