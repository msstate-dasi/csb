package edu.msstate.dasi.csb.distributions

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Represents a probability distribution.
 *
 * @note the resulting distribution is expected to be small, as it is loaded into the driver's memory.
 *
 * @param data the input data on which the distribution will be computed
 * @tparam T the input data type
 */
class Distribution[T: ClassTag](data: RDD[T]) {
  /**
   * The internal representation, an array of `(value, probability)` pairs.
   */
  private val distribution: Array[(T, Double)] = {
    val occurrences = data.map((_, 1L)).reduceByKey(_+_).cache() // Count how many occurrences for each value

    val occurrencesSum = occurrences.values.reduce(_+_) // Compute the total amount of elements
    val distribution = occurrences.mapValues(_ / occurrencesSum.toDouble) // Normalize to obtain probabilities
    val result = distribution.sortBy(_._2).collect() // TODO: do we really need to sort?

    occurrences.unpersist()

    result
  }

  /**
   * Returns a sample of the distribution.
   *
   * @note it is based on a cumulative distribution function.
   */
  def sample: T = {
    var accumulator = 0.0
    var sample = None: Option[T]

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
