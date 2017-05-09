package edu.msstate.dasi.csb

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait Veracity {
  val name: String

  // TODO: we should find a more elegant solution for passing the bucket size to the inheriting class
  protected var globalBucketSize = 0.0

  /**
   * Saves a distribution RDD to a CSV file
   */
  protected def saveDist[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], filename: String, overwrite: Boolean): Boolean = {
    Util.RDDtoCSV(rdd, filename, overwrite)
  }

  /**
   * Computes a normalized bucketed distribution given a list of keys.
   */
  protected def normDistRDD(keys: RDD[Double], bucketSize: Double): RDD[(Double, Double)] = {

    // Computes the sum of keys
    val keysSum = keys.sum()

    // Normalizes keys
    var normKeys = keys.map { key => key / keysSum }

    if (bucketSize > 0) {
      val normBucketSize = bucketSize / keysSum
      // Groups keys in buckets adding their values
      normKeys = normKeys.map { key => key - key % normBucketSize }
      globalBucketSize = normBucketSize
    }

    // Computes how many times each key appears
    val keysCount = normKeys.map { key => (key, 1L) }.reduceByKey(_ + _)

    // Computes the sum of values
    val valuesSum = keysCount.values.sum()

    // Normalizes values
    val normDist = keysCount.map { case (key, value) => (key, value / valuesSum) }

    normDist
  }

  /**
   * Computes the Euclidean distance between the values of two (key,value) RDDs.
   */
  protected def euclideanDistance[V: ClassTag](rdd1: RDD[(V, Double)], rdd2: RDD[(V, Double)]): Double = {
    math.sqrt(
      // Unifies the RDDs
      rdd1.union(rdd2)
        // Computes the squared difference of the values belonging to the same keys
        .reduceByKey((value1, value2) => math.pow(value1 - value2, 2))
        // Sum all the resulting values
        .values.sum()
    )
  }

  /**
   * Computes the veracity factor between two graphs.
   */
  def apply[VD: ClassTag, ED: ClassTag](g1: Graph[VD, ED], g2: Graph[VD, ED], saveDistAsCSV: Boolean = false,
                                        filePrefix: String = "", overwrite: Boolean = false): Double
}
