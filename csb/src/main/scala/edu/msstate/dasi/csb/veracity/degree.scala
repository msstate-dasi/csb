package edu.msstate.dasi.csb.veracity

import edu.msstate.dasi.csb.Util
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.reflect.ClassTag

sealed trait Degree extends Veracity {
  /**
   * Computes the degree veracity factor between the degrees of two graphs.
   */
  protected def degree(d1: VertexRDD[Int], d2: VertexRDD[Int], saveDistAsCSV: Boolean = false, filePrefix: String = "",
             overwrite: Boolean = false): Double = {
    val d1Values = d1.values.map { value => value.toDouble }
    // TODO: Handle the case when the values are less than two
    val d1BucketSize = d1Values.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val d2Values = d2.values.map { value => value.toDouble }
    // TODO: Handle the case when the values are less than two
    val d2BucketSize = d2Values.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min

    val bucketSize = math.min(d1BucketSize, d2BucketSize)

    val d1RDD = normDistRDD(d1Values, bucketSize)
    val d2RDD = normDistRDD(d2Values, bucketSize)

    if (saveDistAsCSV) {
      Util.RDDtoCSV(d1RDD, filePrefix + "_degrees_dist.g1.csv", overwrite)
      Util.RDDtoCSV(d2RDD, filePrefix + "_degrees_dist.g2.csv", overwrite)
    }

    val bucketNum = 1.0 / globalBucketSize

    euclideanDistance(d1RDD, d2RDD) / bucketNum
  }
}

object Degree extends Degree {
  val name: String = "Degree"

  /**
   * Computes the degree veracity factor between two graphs.
   */
  def apply[VD: ClassTag, ED: ClassTag](g1: Graph[VD, ED], g2: Graph[VD, ED], saveDistAsCSV: Boolean = false,
                                                 filePrefix: String = "", overwrite: Boolean = false): Double = {
    degree(g1.degrees, g2.degrees, saveDistAsCSV, filePrefix, overwrite)
  }
}
object InDegree extends Degree {
  val name: String = "In-degree"
  /**
   * Computes the in-degree veracity factor between two graphs.
   */
  def apply[VD: ClassTag, ED: ClassTag](g1: Graph[VD, ED], g2: Graph[VD, ED], saveDistAsCSV: Boolean = false,
                                                 filePrefix: String = "", overwrite: Boolean = false): Double = {
    degree(g1.inDegrees, g2.inDegrees, saveDistAsCSV, filePrefix, overwrite)
  }
}
object OutDegree extends Degree {
  val name: String = "Out-degree"

  /**
   * Computes the out-degree veracity factor between two graphs.
   */
  def apply[VD: ClassTag, ED: ClassTag](g1: Graph[VD, ED], g2: Graph[VD, ED], saveDistAsCSV: Boolean = false,
                                                 filePrefix: String = "", overwrite: Boolean = false): Double = {
    degree(g1.outDegrees, g2.outDegrees, saveDistAsCSV, filePrefix, overwrite)
  }
}
