package edu.msstate.dasi.csb.distributions

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Represents a probability distribution.
 *
 * @note the input probabilities should be in descending order to maximize sampling performance.
 * @param distribution the array containing the distribution
 * @tparam T the distribution data type
 */
class Distribution[T](distribution: Array[(T, Double)]) extends Serializable {
  /**
   * Returns a sample of the distribution.
   *
   * @note it is based on a cumulative distribution function.
   */
  def sample: T = {
    var accumulator = 0.0
    var sample = Option.empty[T]

    val r = Random.nextDouble()
    val it = distribution.iterator

    while (accumulator <= r && it.hasNext) {
      val (value, prob) = it.next()

      sample = Some(value)
      accumulator += prob
    }

    sample.get
  }
}

/**
 * The [[Distribution]] object contains helper methods to build [[Distribution]] instances from RDDs.
 */
object Distribution {
  /**
   * Builds a distribution instance from an [[RDD]] of values.
   *
   * @note the resulting distribution is expected to be small, as it will be loaded into the driver's memory.
   * @param data the input [[RDD]] data
   * @tparam T the input data type
   *
   * @return the resulting [[Distribution]] object
   */
  def apply[T: ClassTag](data: RDD[T]): Distribution[T] = {
    val occurrences = data.map((_, 1L)).reduceByKey(_ + _) // Count how many occurrences for each value
      .sortBy(_._2, ascending = false) // Descending order to maximize sampling performance
      .cache()

    val occurrencesSum = occurrences.values.reduce(_ + _) // Compute the total amount of elements
    val distribution = occurrences.mapValues(_ / occurrencesSum.toDouble) // Normalize to obtain probabilities
    val result = distribution.collect()

    occurrences.unpersist()

    new Distribution(result)
  }
}
