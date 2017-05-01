package edu.msstate.dasi.csb

case class Config(
                 mode: String = "",
                 partitions: Int = sc.defaultParallelism,
                 backend: String = "fs",

                 seedGraphPrefix: String = "seed",
                 synthGraphPrefix: String = "synth",

                 alertLog: String = "alert",
                 connLog: String = "conn.log",
                 outLog: String = "out.log",

                 synthesizer: String = "",
                 skipProperties: Boolean = false,
                 iterations: Int = 0,

                 sampleFraction: Double = 0.1,

                 seedMatrix: String = "seed.mtx",

                 metrics: Seq[String] = Seq("all")
                 )
