package edu.msstate.dasi

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

/**
  * Kro(n)Fit
  * Created by B1nary on 1/27/2017.
  */
class KroFit(sc: SparkContext, partitions: Int, initMtxStr: String, gradIter: Int, connLog: String) extends DataParser {

    def run(): Unit = {
      val initKronMtx = new kronMtx(sc, Array(Array(.9,.7,.5,.2)))

      println("INIT PARAM")
      initKronMtx.dump()


    }

}
