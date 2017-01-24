package edu.msstate.dasi

import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.reflect.ClassTag

/**
 * Created by scordio on 1/24/17.
 */
object PageRank extends Veracity {
  /**
   * Computes the pageRank veracity factor between two graphs.
   */
  override def run[VD: ClassTag, ED: ClassTag](g1: Graph[VD, ED], g2: Graph[VD, ED], saveDistAsCSV: Boolean = false,
                                               filePrefix: String = "", overwrite: Boolean = false): Double = {
    val pageRankTolerance = 0.001

    // Computes the bucket size as the minimum difference between any successive pair of ordered values
    val seedPrRDD = g1.pageRank(pageRankTolerance).vertices.values
    val seedBucketSize = seedPrRDD.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val synthPrRDD = g2.pageRank(pageRankTolerance).vertices.values
    val synthBucketSize = synthPrRDD.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val bucketSize = math.min(seedBucketSize, synthBucketSize)

    val seedRDD = normDistRDD(seedPrRDD, bucketSize)
    val synthRDD = normDistRDD(synthPrRDD, bucketSize)

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_page_rank_dist.g1.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_page_rank_dist.g2.csv", overwrite)
    }

    val bucketNum = 1.0 / globalBucketSize

    euclideanDistance(seedRDD, synthRDD) / bucketNum
  }
}
