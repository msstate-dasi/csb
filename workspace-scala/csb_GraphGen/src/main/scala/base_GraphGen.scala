import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.rdd.RDD


/**
  * Created by spencer on 11/3/16.
  */
class base_GraphGen {
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx.{Graph, VertexRDD}
  import org.apache.spark.rdd.RDD

  def printGraph(theGraph: Graph[nodeData, edgeData]) = {
    val facts: RDD[String] = theGraph.triplets.map(record =>
      record.srcId + " " + record.dstId + " " + record.attr)
    println("Graph:")
    facts.foreach(println)
    println()

    val nodeDegs: VertexRDD[Int] = theGraph.degrees
    //println("(ID, Degree):")
    //nodeDegs.foreach(println)
    //println()

    val degList: RDD[(Int, Int)] = nodeDegs.map(record => (record._2,1)).reduceByKey(_+_)
    //println("(Deg, Number of Nodes with Deg")
    //degList.foreach(println)
    //println()

    (facts, nodeDegs, degList)
  }

  def saveGraph(sc: SparkContext, theGraph: Graph[nodeData, edgeData], path: String): Unit = {
    val facts: RDD[String] = sc.parallelize(Array("Source,Target,Weight")).union(theGraph.triplets.map(record =>
      record.srcId + "," + record.dstId + "," + record.attr))
    try {
      facts.repartition(1).saveAsTextFile(path)
    } catch {
      case e: Exception => println("Couldn't save file " + path)
    }
  }

  def inDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.inDegrees.map(record => (record._2,1)).reduceByKey(_+_)
  }
  
  def outDegreesDist(theGraph: Graph[nodeData, edgeData]): RDD[(Int,Int)] = {
    theGraph.outDegrees.map(record => (record._2,1)).reduceByKey(_+_)
  }
}
