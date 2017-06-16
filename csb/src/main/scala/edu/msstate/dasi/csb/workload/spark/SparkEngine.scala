package edu.msstate.dasi.csb.workload.spark

class SparkEngine {
  /**
   * Passing this function as a parameter of RDD::foreach() forces the computation of the RDD without adding
   * noticeable overhead.
   */
  def doNothing(x: Any): Unit = {}
}
