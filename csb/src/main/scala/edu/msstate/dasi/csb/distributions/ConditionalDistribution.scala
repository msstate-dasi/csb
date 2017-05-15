package edu.msstate.dasi.csb.distributions

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Represents a conditional probability distribution.
 *
 * @note the resulting distribution is expected to be small, as it is loaded into the driver's memory.
 *
 * @param data the input data on which the distribution will be computed
 * @tparam T the input data type
 * @tparam Conditioner the data type of the conditioning value
 */
class ConditionalDistribution[T: ClassTag, Conditioner: ClassTag](data: RDD[(T, Conditioner)]) {

  /**
   * The internal representation, a Map of `Conditioner` values to [[Distribution]] objects.
   */
  private val distributions: mutable.Map[Conditioner, Distribution[T]] = {

    var distributions = mutable.Map.empty[Conditioner, Distribution[T]]

    val inputData = data.cache()

    for (conditionalValue <- inputData.values.distinct.toLocalIterator) {
      // For each conditioning value, create a Distribution object
      val conditionedData = inputData.filter{ case (_, conditioner) => conditioner == conditionalValue }.keys
      distributions += (conditionalValue -> new Distribution(conditionedData))
    }

    inputData.unpersist()

    distributions
  }

  /**
   * Returns a sample of the distribution given the conditioning value.
   */
  def sample(conditioner: Conditioner): T = {
    distributions(conditioner).sample
  }
}
