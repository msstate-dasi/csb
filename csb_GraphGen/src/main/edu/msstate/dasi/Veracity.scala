package edu.msstate.dasi

import java.io.{BufferedWriter, File}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.reflect.ClassTag

/**
 * Helper methods to compute veracity metrics for [[org.apache.spark.graphx.Graph]]
 */
object Veracity extends data_Parser {
  var globalBucketSize = 0.0
  /**
   * Computes a normalized bucketed distribution given a list of keys
   *
   * @param keys Keys to analyze
   * @return RDD containing the normalized distribution
   */
  private def normDistRDD(keys: RDD[Double], bucketSize: Double): RDD[(Double, Double)] = {

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
    val keysCount = normKeys.map { key =>  (key, 1L) }.reduceByKey(_ + _).cache()

    // Computes the sum of values
    val valuesSum = keysCount.values.sum()

    // Normalizes values
    val normDist = keysCount.map { case (key, value) => (key, value / valuesSum) }

    normDist
  }

  /**
   * Squashes saveAsTextFile() part files together into a single file using the Hadoop’s merge function
   *
   * @param srcPath
   * @param dstPath
   *
   * @note This is a better approach rather than rdd.coalesce(1).saveAsTextFile() because the latter will put all
   *       final data through a single reduce task, with no parallelism and the risk of overloading an executor
   */
  private def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  /**
   * Save a (key,value) RDD to a CSV file
   *
   * @param rdd
   * @param filename
   * @param overwrite
   */
  private def RDDtoCSV[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], filename: String, overwrite :Boolean): Unit = {
    val tmpFile = "__RDDtoCSV.tmp"
    val numPartitions = 16

    if (overwrite) {
      FileUtil.fullyDelete(new File(filename))
    }

    rdd.map {
      // for each (key,value) pair, create a "key,value" string
      case (key, value) => Array(key, value).mkString(",")
    }.coalesce(numPartitions).saveAsTextFile(tmpFile)

    merge(tmpFile, filename)

    FileUtil.fullyDelete(new File(tmpFile))
  }

  /**
   * Computes the Euclidean distance between two RDDs
   */
  private def euclideanDistance(rdd1: RDD[(Double, Double)], rdd2: RDD[(Double, Double)]): Double = {
    math.sqrt(
      // Unifies the RDDs
      rdd1.union(rdd2)
      // Computes the squared difference of the values belonging to the same keys
      .reduceByKey( (value1, value2) => math.pow(value1-value2, 2) )
      // Sum all the resulting values
      .values.sum()
    )
  }

  /**
   * Computes the degree veracity factor between the degrees of two graphs
   *
   * @param seed
   * @param synth
   * @param saveDistAsCSV
   * @param filePrefix
   * @param overwrite
   */
  def degree(seed: VertexRDD[Int], synth: VertexRDD[Int], saveDistAsCSV : Boolean = false, filePrefix: String = "",
              overwrite :Boolean = false): Double = {

    val seedValues = seed.values.map { value => value.toDouble }.cache()
    val seedBucketSize = seedValues.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min()

    val synthValues = synth.values.map { value => value.toDouble }.cache()
    val synthBucketSize = synthValues.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min()

    val bucketSize = math.min(seedBucketSize, synthBucketSize)

    val seedRDD = normDistRDD(seedValues, bucketSize).cache()
    val synthRDD = normDistRDD(synthValues, bucketSize).cache()

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_degrees_dist.seed.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_degrees_dist.synth.csv", overwrite)
    }

    val bucketNum = 1.0 / globalBucketSize

    euclideanDistance(seedRDD,synthRDD) / bucketNum
  }

  /**
   * Returns the shortest directed-edge path from src to dst in the graph. If no path exists, returns
   * the empty list.
   */
  private def bfs[VD, ED](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Seq[VertexId] = {
    if (src == dst) return List(src)

    // The attribute of each vertex is (dist from src, id of vertex with dist-1)
    var g = graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L)).cache()

    // Traverse forward from src
    var dstAttr = (Int.MaxValue, 0L)
    while (dstAttr._1 == Int.MaxValue) {
      val msgs = g.aggregateMessages[(Int, VertexId)](
        e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
          e.sendToDst((e.srcAttr._1 + 1, e.srcId))
        },
        (a, b) => if (a._1 < b._1) a else b).cache()

      if (msgs.count == 0) return List.empty

      g = g.ops.joinVertices(msgs) {
        (id, oldAttr, newAttr) =>
          if (newAttr._1 < oldAttr._1) newAttr else oldAttr
      }.cache()

      dstAttr = g.vertices.filter(_._1 == dst).first()._2
    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

    path
  }

  /**
   * Computes the effective diameter of the graph, defined as the minimum number of links (steps/hops) in which some
   * fraction (or quantile q, say q = 0.9) of all connected pairs of nodes can reach each other
   *
   * @return The value of the effective diameter
   */
  def pageRank(seed: Graph[nodeData, edgeData], synth: Graph[nodeData, edgeData], saveDistAsCSV : Boolean = false,
               filePrefix: String = "", overwrite: Boolean = false): Double = {
    val tolerance = 0.001

    // Computes the bucket size as the minimum difference between any successive pair of ordered values

    val seedPrResult = seed.pageRank(tolerance).vertices.values
    val seedBucketSize = seedPrResult.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min()

    val synthPrResult = synth.pageRank(tolerance).vertices.values
    val synthBucketSize = synthPrResult.distinct.sortBy(identity).sliding(2).map { case Array(x, y) => y - x }.min()

    val bucketSize = math.min(seedBucketSize, synthBucketSize)

    val seedRDD = normDistRDD(seedPrResult, bucketSize).cache()
    val synthRDD = normDistRDD(synthPrResult, bucketSize).cache()

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_page_rank_dist.seed.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_page_rank_dist.synth.csv", overwrite)
    }

    val bucketNum = 1.0 / globalBucketSize

    euclideanDistance(seedRDD,synthRDD) / bucketNum
  }

//  /**
//   * Computes the effective diameter of the graph, defined as the minimum number of links (steps/hops) in which some
//   * fraction (or quantile q, say q = 0.9) of all connected pairs of nodes can reach each other
//   *
//   * @param graph The graph to analyze
//   * @return The value of the effective diameter
//   */
//  def effectiveDiameter(graph: Graph[nodeData, edgeData]): Double = {
//    0
//  }

  class DistanceNodePair(var distance: Long, var totalPairs: Long) extends Comparable[DistanceNodePair] {

    override def compareTo(dp: DistanceNodePair): Int = (this.distance - dp.distance).toInt
  }

  class NodeVisitCounter extends java.io.Serializable {

    var totalPairs: Long = _

    var levelSize: mutable.HashMap[Long, Long] = _ //first is distance second is pair at that distance
  }


  /**
    * Computes the effective diameter of a graph.
    * @param distancePairs
    * @param totalPairs
    * @return
    */
  def computeEffectiveDiameter(distancePairs: ArrayBuffer[DistanceNodePair], totalPairs: Long): Double = {
    var effectiveDiameter = 0.0
    for (i <- distancePairs.indices)
    {
      val dp = distancePairs(i)
      val ratio = dp.totalPairs.toDouble / totalPairs.toDouble
      if (ratio >= 0.9) {
        if (ratio == 0.9 || i == 0) return dp.distance else {
          val dpPrev = distancePairs(i - 1)
          val prevRatio = dpPrev.totalPairs.toDouble / totalPairs.toDouble
          val slope = (dp.distance.toDouble - dpPrev.distance.toDouble) / (ratio - prevRatio)
          effectiveDiameter = slope * (0.9 - prevRatio) + dpPrev.distance.toDouble
          return effectiveDiameter
        }
      }
    }
    effectiveDiameter = distancePairs(distancePairs.size - 1).distance
    effectiveDiameter
  }

  /**
    * This function is call when distance node pairs needs to be computed to compute the effective diameter
    * @param sc spark context
    * @param graph the graph to compute the effective diameter
    * @param partitions number of partitions for spark rdd's
    * @return
    */
  def computeEffectiveDiameter(sc: SparkContext, graph: Graph[nodeData, edgeData], partitions: Int): Double =
  {
    val (distanceNodePair, totalPairs) = getNodePairDistances(sc, graph, partitions)
    computeEffectiveDiameter(distanceNodePair, totalPairs)
  }


  /**
    * Performs a bredth first search.  This algorithm is run in parallel
    * @param nID node id
    * @param hashmap the hashmap that gives the children for every node
    * @return
    */
  def BFSNode(nID: Long, hashmap: Broadcast[collection.Map[Long, Array[Long]]]): NodeVisitCounter = {


    val q = new Queue[Long]()
    q.enqueue(nID)
    val visited = new mutable.HashSet[VertexId]()
    val levelSize = new mutable.HashMap[Long, Long]()
    visited.add(nID)
    var totalPairs: Long = 0
    val visitCounter = new NodeVisitCounter()
    var level = 0
    while(q.nonEmpty)
    {
      val size = q.size
      totalPairs += size
      if(level != 0)
      {
        levelSize.put(level, size);
      }

      var list: Array[Long] = new Array[Long](size)
      for(x <- 0 until size)
      {
        list(x) = q.dequeue()
      }
      var children: Array[Long] = null
      for(x <- list)
      {

        var node: Long = x

        children = hashmap.value.get(x).head
        for(c: Long <- children)
        {
          val childNode = hashmap.value.get(c).head
          if(!visited.contains(c))
          {
              q.enqueue(c)
              visited.add(c)
          }
        }
      }
      level += 1
    }
    totalPairs -= 1

    visitCounter.levelSize = levelSize
    visitCounter.totalPairs = totalPairs

    return visitCounter
  }


  /**
    * This takes two hashmaps and combines them.  Arindam got this from stackoverflow
    * @param map1
    * @param map2
    * @return
    */
  def mergeMaps(map1: mutable.HashMap[Long, Long], map2: mutable.HashMap[Long, Long]): mutable.HashMap[Long, Long] = {
    val merged = new mutable.HashMap[Long, Long]()
    for (x <- map1.keySet)
    {
      val y = map2.get(x)

      if (y == None)
      {
        merged.put(x, map1.get(x).head)
      }
      else
      {
        merged.put(x, map1.get(x).head + y.head)
      }
    }
    for (x <- map2.keySet if merged.get(x) == None)
    {
      merged.put(x, map2.get(x).head)
    }
    return merged
  }

  /**
    * performs hop plot
    * @param sc spark context
    * @param filename file to save data once computed
    * @param graph the graph to evaluate the hop plot
    * @param partitions the number of paritions for spark rdd's
    */
  def HopPlot(sc: SparkContext, filename: String, graph: Graph[nodeData, edgeData], partitions: Int)
  {


    val (distancePairs, totalPairs) = getNodePairDistances(sc, graph, partitions)

    println("Writting to file")
    val file = new File(filename)
    val bw = new BufferedWriter(new java.io.FileWriter(file))

    //uncomment if you want to view the data to stdout
//    for (dp: DistanceNodePair <- distancePairs)
//    {
//      bw.write("Distance " + dp.distance + ", " + dp.totalPairs + "\n")
//    }
    bw.write("\n")
    bw.write("Effective Diameter(Average Hop): " + computeEffectiveDiameter(distancePairs, totalPairs).toString)
    bw.close()
    println("Done writting to file")
  }




  /**
    * This function computes all the distances between nodes in the graph (it does this by running BFS on every node
    * @param sc spark context
    * @param graph the graph the get the node distances from
    * @param partitions the number of paritions for spark rdd's
    *  @return an array of the distance and number of nodes that can be reached at the distance along with the total number of pairs in the graph
    */
  def getNodePairDistances(sc: SparkContext, graph: Graph[nodeData, edgeData], partitions: Int): (ArrayBuffer[DistanceNodePair], Long) =
  {
    val allEdges = getAllEdges(sc, graph.edges)
    var totalPairs: Long = 0
    val broadcastNodeSet = sc.broadcast(allEdges.collectAsMap())

    println("done prepping for hop plot")


    //paralize doing BFS
    println("doing BFS search please wait....")
    val BFSNodes = allEdges.map(record => record._1).repartition(partitions).map(record => BFSNode(record, broadcastNodeSet)).persist()
    BFSNodes.count() //forces the BFS to execute
    println("done")


    //after it is over count all the pairs
    totalPairs = BFSNodes.map(record => record.totalPairs).reduce(_ + _) / 2 //not required but to be correct divide the pairs by two since each pair is counted twice
    println("total pairs " + totalPairs)


    var levelSizeMerged = BFSNodes.map(record => record.levelSize).reduce((record1, record2) => this.mergeMaps(record1, record2))

    println(levelSizeMerged.size)
    var distancePairs = new ArrayBuffer[DistanceNodePair]()

    for (x <- levelSizeMerged.keySet)
    {
      println("Node pairs " + levelSizeMerged.get(x).head + ", " + "Distance " + x)
      val dp = new DistanceNodePair(x, levelSizeMerged.get(x).head/ 2) //not required but to be correct divide the pairs by two since each pair is counted twice
      distancePairs.append(dp)
    }
    distancePairs = distancePairs.sortBy(_.distance)
    for (i <- 1 until distancePairs.size)
    {
      val dp = distancePairs(i)
      dp.totalPairs += distancePairs(i - 1).totalPairs
    }

    return (distancePairs, totalPairs)

  }

  /**
    * returns a rdd of undirected edges (edges are directed in spark)
    * @param sc spark context
    * @param inEdges the edges in a graph
    * @return
    */
  def getAllEdges(sc: SparkContext, inEdges: EdgeRDD[edgeData]): RDD[(Long, Array[Long])]  =
  {
    //Here is how this works
    //first we grab every edge in the list and make it undirected
    //EXAMPLE: say we have the graph (1,2) the flatmap converts this to (1,2),(2,1) thus making it undirected
    //Next we group by key which does groups vertices to their out vertices
    //EXAMPLE: Say we have the graph (1,2), (1,3), (3,1) this will convert it to (1,Array(2,3)), (3,1) (this is an invalid example since the edges are directed but you get the point it groups)
    //next we count the undirectedEdges for no other reason than to force the RDD in memory
    val undirectedEdges = inEdges.flatMap(record => Array((record.srcId, record.dstId), (record.dstId, record.srcId)))
      .groupByKey().map(record => (record._1, record._2.toArray)).persist()
    undirectedEdges.count()
    return undirectedEdges
  }

  /**
    * This is the wrapper function to perform hop plot
    * @param sc spark context
    * @param filename the file to save the data
    * @param seedGraph the seed graph
    * @param synthGraph the synthetically generated graph
    * @param partitions the number of paritions to set spark rdd's
    */
  def hopPlotMetric(sc: SparkContext, filename: String, seedGraph: Graph[nodeData, edgeData], synthGraph: Graph[nodeData, edgeData], partitions: Int): Unit =
  {
    println("performing hop plot on seed graph")
    HopPlot(sc, "seed_" + filename, seedGraph, partitions)
    println("done performing hop plot on seed graph")
    println("performing hop plot on synth graph")
    HopPlot(sc, "synth_" + filename, synthGraph, partitions)
    println("done performing hop plot on synth graph")
  }

  /**
    * The wrapper funtion for computing effective diamter of a graph
    * @param sc spark context
    * @param seedGraph the seed graph
    * @param synthGraph the synthetically generated graph
    * @param partitions the number of paritions to set spark rdd's
    */
  def effectiveDiameterMetric(sc: SparkContext, seedGraph: Graph[nodeData, edgeData], synthGraph: Graph[nodeData, edgeData], partitions: Int, filename: String): Unit =
  {
    println("computing effective diameter on seed graph")
    val seedeffectDia = computeEffectiveDiameter(sc, seedGraph, partitions)
    println(seedeffectDia)
    println("computing effective diameter on synth graph")
    val syntheffectDia = computeEffectiveDiameter(sc, synthGraph, partitions)
    println(syntheffectDia)

    println("Writing to file")
    val file = new File(filename)
    val bw = new BufferedWriter(new java.io.FileWriter(file))
    bw.write("seed diameter: " + seedeffectDia)
    bw.write("\nsynth diameter: " + syntheffectDia)
    bw.close()
    println("Finished writting to file")
  }
  def degreeMetric(sc: SparkContext, seedGraph: Graph[nodeData, edgeData], synthGraph: Graph[nodeData, edgeData], partitions: Int, filename: String): Unit =
  {
    val degVeracity = degree(seedGraph.degrees, synthGraph.degrees)
    val inDegVeracity = degree(seedGraph.inDegrees, synthGraph.inDegrees)
    val outDegVeracity = degree(seedGraph.outDegrees, synthGraph.outDegrees)

    println("Finished calculating degrees veracity.\n\tDegree Veracity:" + degVeracity + "\n\tIn Degree Veracity: " +
      inDegVeracity + "\n\tOut Degree Veracity:" + outDegVeracity)

    println("writing to file")
    val file = new File(filename)
    val bw = new BufferedWriter(new java.io.FileWriter(file))
    bw.write("Finished calculating degrees veracity.\n\tDegree Veracity:" + degVeracity + "\n\tIn Degree Veracity: " +
      inDegVeracity + "\n\tOut Degree Veracity:" + outDegVeracity)

    bw.close()
    println("Finished writting to file")
  }
}
