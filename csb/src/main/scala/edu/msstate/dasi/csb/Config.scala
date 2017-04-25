package edu.msstate.dasi.csb

case class Config(
                 mode: String = "",
                 partitions: Int = sc.defaultParallelism,

                 inGraphPrefix: String = "",
                 outGraphPrefix: String = "",

                 alertLog: String = "alert",
                 connLog: String = "conn.log",
                 augLog: String = "aug.log",
                 seedPrefix: String = "seed"
                 )
