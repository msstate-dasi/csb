package edu.msstate.dasi

import java.io.{BufferedWriter, File}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.reflect.ClassTag

/**
 * Helper methods to compute veracity metrics for [[org.apache.spark.graphx.Graph]]
 */
object Veracity extends data_Parser {

  /**
   * Computes a normalized distribution given a list of keys
   *
   * @param keys Keys to analyze
   * @param bucketNum Number of buckets where the keys should fall; if 0, the keys are not bucketed
   * @return RDD containing the normalized distribution
   */
  private def normDistRDD(keys: VertexRDD[Int], bucketNum :Int = 0 ): RDD[(Double, Double)] = {
    // Computes how many times each key appears
    val keysCount = keys.map { case (_, key)  => (key, 1L) }.reduceByKey(_ + _).cache()

    // Computes the sum of keys and the sum of values
    val keysSum = keysCount.keys.sum()
    val valuesSum = keysCount.values.sum()

    // Normalizes keys and values
    var normDist = keysCount.map { case (key, value) => (key / keysSum, value / valuesSum) }

    if (bucketNum > 0) {
      val bucketSize = 1.0 / bucketNum
      // Groups keys in buckets adding their values
      normDist = normDist.map { case (key, value) => (key - key % bucketSize, value) }.reduceByKey(_ + _)
    }

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
    val bucketNum = 100000

    val seedRDD = normDistRDD(seed,bucketNum).cache()
    val synthRDD = normDistRDD(synth, bucketNum).cache()

    if (saveDistAsCSV) {
      RDDtoCSV(seedRDD, filePrefix + "_degrees_dist.seed.csv", overwrite)
      RDDtoCSV(synthRDD, filePrefix + "_degrees_dist.synth.csv", overwrite)
    }

    euclideanDistance(seedRDD,synthRDD) / bucketNum
  }

//  /**
//   * Computes the effective diameter of the graph, defined as the minimum number of links (steps/hops) in which some
//   * fraction (or quantile q, say q = 0.9) of all connected pairs of nodes can reach each other
//   *
//   * @param graph The graph to analyze
//   * @return The value of the effective diameter
//   */
//  def effectiveDiameter(graph: Graph[nodeData, edgeData]): Long = {
//    0
//  }




  class DistanceNodePair(var distance: Long, var totalPairs: Long) extends Comparable[DistanceNodePair] {

    override def compareTo(dp: DistanceNodePair): Int = (this.distance - dp.distance).toInt
  }

  class Node(var id: VertexId) extends java.io.Serializable {

    var children: ArrayBuffer[Long] = new ArrayBuffer[Long]()

    def addChildren(c: Long) {
      children.append(c)
    }
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
    for (i <- 0 until distancePairs.size) //until stops 1 before distancePairs.size
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
    * Performs a bredth first search.  This algorithm is run in parallel
    * @param n
    * @param hashmap
    * @return
    */
  def BFSNode(n: Node, hashmap: scala.collection.Map[Long, Node]): NodeVisitCounter = {


    val q = new Queue[Node]()
    q.enqueue(n)
    val visited = new mutable.HashSet[VertexId]()
    val levelSize = new mutable.HashMap[Long, Long]()
    visited.add(n.id)
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

      var list: Array[Node] = new Array[Node](size)
      for(x <- 0 until size)
      {
        list(x) = q.dequeue()
      }
      var children: ArrayBuffer[Long] = null
      for(x <- list)
      {

        var node: Node = x

        children = node.children
        for(c: Long <- children)
        {
          val childNode = hashmap.get(c).head
          if(!visited.contains(childNode.id))
          {
            q.enqueue(childNode)
            visited.add(childNode.id)
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

  def HopPlot(sc: SparkContext, nodes: Array[(Node, Array[Node])], hashmap: scala.collection.Map[Long, Node], filename: String)
  {
    //    var levelSizeMerged = new mutable.HashMap[Long, Long]()
    var totalPairs: Long = 0

    //paralize doing BFS
    println("doing BFS search please wait....")
    val BFSNodes = sc.parallelize(nodes).map(record => BFSNode(record._1, hashmap)).persist()
    BFSNodes.count()
    println("done")
    //get total number of pairs (this number is messed up
    totalPairs = BFSNodes.map(record => record.totalPairs).reduce(_ + _) / 2 //not required but to be correct divide the pairs by two since each pair is counted twice
    println("total pairs " + totalPairs)
    var levelSizeMerged = BFSNodes.map(record => record.levelSize).reduce((record1, record2) => this.mergeMaps(record1, record2))

    //    for (node <- nodes)
    //    {
    //      val counter = BFSNode(node)
    //      totalPairs += counter.totalPairs
    //      val merged = mergeMaps(levelSizeMerged, counter.levelSize)
    //      levelSizeMerged = merged
    //    }
    println(levelSizeMerged.size)
    var distancePairs = new ArrayBuffer[DistanceNodePair]()

    for (x <- levelSizeMerged.keySet)
    {
      println("Node pairs " + levelSizeMerged.get(x).head + ", " + "Distance " + x)
      val dp = new DistanceNodePair(x, levelSizeMerged.get(x).head/ 2) //not required but to be correct divide the pairs by two since each pair is counted twice
      distancePairs.append(dp)
    }
    distancePairs = distancePairs.sortBy(_.distance)
    //    Collections.sort(distancePairs)
    for (i <- 1 until distancePairs.size)
    {
      val dp = distancePairs(i)
      dp.totalPairs += distancePairs(i - 1).totalPairs
    }

    //write info to file
//    val fw = new FileWriter(new File("hop-plotData"))
//    val bw = new BufferedWriter(fw.osw)

    val file = new File(filename)
    val bw = new BufferedWriter(new java.io.FileWriter(file))

    for (dp: DistanceNodePair <- distancePairs)
    {
      bw.write("Distance " + dp.distance + ", " + dp.totalPairs + "\n")
//      println("Distance " + dp.distance + ", " + dp.totalPairs)
    }
    bw.write("\n")
    bw.write(computeEffectiveDiameter(distancePairs, totalPairs).toString)
    bw.close()
//    println(computeEffectiveDiameter(distancePairs, totalPairs))
  }


  def performHopPlot(sc: SparkContext, seedVertFile: String, seedEdgeFile: String, saveFile: String): Unit =
  {
    println()
    println("Loading seed graph with vertices file: " + seedVertFile + " and edges file " + seedEdgeFile + " ...")

    var startTime = System.nanoTime()
    //read in and parse vertices and edges
    val (inVertices, inEdges) = readFromSeedGraph(sc, seedVertFile,seedEdgeFile)
    var timeSpan = (System.nanoTime() - startTime) / 1e9
    println()
    println("Finished loading seed graph.")
    println("\tTotal time elapsed: " + timeSpan.toString)
    println("\tVertices "+inVertices.count())
    println("\tEdges "+inEdges.count())
    println()
    println()
    println("running hop plot...")
    println()

    println("prepping for hop plot...")
    //this gets every edge both directions
    val undirectedEdges = inEdges.flatMap(record => Array((record.srcId, record.dstId), (record.dstId, record.srcId))).groupByKey().map(record => (record._1, record._2.toArray))

    //this acts as a hashmap where the key in the vertex id and value is the node
    val hashmap = undirectedEdges.map(record => (record._1, new Node(record._1))).collectAsMap()

    //this creates the children to point back to the hashmap
    val completedNodes = undirectedEdges.map(record => (hashmap.get(record._1).head, for(x <- record._2) yield hashmap.get(x).head)).collect()
    completedNodes.map(record => for(x <- record._2) {record._1.children.append(x.id); hashmap.get(record._1.id).head.children.append(x.id);})

    println("done prepping for hop plot")
    //do hop plot
    HopPlot(sc, completedNodes, hashmap, saveFile)
  }
}
