package edu.msstate.dasi

import java.io.{FileInputStream, ObjectInputStream}

import org.apache.spark.{SparkContext, sql}
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.Random
import collection.JavaConversions._

/**
  * Created by justin on 12/5/2016.
  */

import scala.io.Source

object data_Generator extends Serializable {


  var edgeDistro:       java.util.HashMap[String,Double] = null
  var origBytes:        java.util.HashMap[String,Double] = null
  var protocol:         java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var connectionState:  java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var Duration:         java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var origIPBytesCount: java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var origPacketCount:  java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var respByteCount:    java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var respIPByteCount:  java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var respPacketCount:  java.util.HashMap[String, java.util.HashMap[String, Double]] = null
  var bucketSize: Int = 0
  var spark: SparkSession = null
  var df: sql.DataFrame = null



  /***
    * This acts as the constructor for this object.  THIS MUST BE CALLED FOR THE OBJECT TO FUNCTION PROPERLY
    */
  def init()
  {
    var fis = new FileInputStream("edgeDistr.ser")
    var ois = new ObjectInputStream(fis)
    edgeDistro = ois.readObject().asInstanceOf[java.util.HashMap[String,Double]]

    fis = new FileInputStream("OrigBytes.ser")
    ois = new ObjectInputStream(fis)
    origBytes = ois.readObject().asInstanceOf[java.util.HashMap[String,Double]]

    fis = new FileInputStream("connState.ser")
    ois = new ObjectInputStream(fis)
    connectionState = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("connType.ser")
    ois = new ObjectInputStream(fis)
    protocol = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("durationDist.ser")
    ois = new ObjectInputStream(fis)
    Duration = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("origIPByteCount.ser")
    ois = new ObjectInputStream(fis)
    origIPBytesCount = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("origPacketCount.ser")
    ois = new ObjectInputStream(fis)
    origPacketCount = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("respByteCount.ser")
    ois = new ObjectInputStream(fis)
    respByteCount = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("respIPByteCount.ser")
    ois = new ObjectInputStream(fis)
    respIPByteCount = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    fis = new FileInputStream("respPacketCount.ser")
    ois = new ObjectInputStream(fis)
    respPacketCount = ois.readObject().asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Double]]]

    val firstBucket = origBytes.keySet().head
    val first = firstBucket.split("-")(0)
    val last = firstBucket.split("-")(1)
    bucketSize = last.toInt - first.toInt + 1
  }


  /***
    * The "bucket" is basically the key inside original bytes that will match up with the given parameter.
    * EX: Assumeing bucket size of 10 so "0-9", "10-19" are valid keys, an input of 5 should return "0-9"
    * @param byteCount the count that you want to find the range for
    * @return
    */
  def getBucket(byteCount: Long): String =
  {
    val multiple = byteCount / bucketSize
    val beginBucket = (bucketSize * multiple).toString
    val endBucket = ((bucketSize * multiple) + bucketSize - 1).toString

    return beginBucket + "-" + endBucket
  }

  /***
    * This function returns a random number with a given distribution that isn't dependent upon the value of originalByteCount (eg. edge # and originalByteCount)
    * @param hashTable the hashtable that contains the distribution
    * @return random number from distribution
    */
  def getIndependentVariable(hashTable : java.util.HashMap[String, Double]): Long =
  {
    val r : Random = Random
    val randNum = r.nextDouble()

    var sum = 0.0
    for(key: String <- hashTable.keySet())
    {
      sum = sum + hashTable.get(key)
      if(sum >= randNum)
        {
          if(key.contains("-")) return randNumFromRange(key)
          else return key.toInt
        }
    }
    return 1
  }

  /***
    * Given a range specified by str we return a random number within that range
    * @param str a range in the format "a-b" where a < b and both are integers
    * @return
    */
  def randNumFromRange(str: String): Long =
  {
    val first = str.split("-")(0).toLong
    val last = str.split("-")(1).toLong
    val r = new Random()

    return r.nextInt(last.toInt - first.toInt) + first
  }


  /***
    * This function returns a random number given a variable that is dependent upon original byte count
    * @param byteNum the original byte count
    * @param hashTable the hashtable that contains all the information about this property distribution
    * @param number should the function return the number string or keep it as it is(basically should it pick a random number specified in the range or return that string)
    * @return
    */
  def getDependentVariable(byteNum: Long, hashTable : java.util.HashMap[String, java.util.HashMap[String, Double]], number: Boolean): String =
  {
    val r : Random = Random
    val randNum = r.nextDouble()
    var sum = 0.0


    val bucket = getBucket(byteNum)
    val subHashTable = hashTable.get(bucket)

    for(key: String <- subHashTable.keySet())
      {
        sum = sum + subHashTable.get(key)
        if(sum >= randNum)
          {
            if(!number) return key
            else return randNumFromRange(key).toString
          }
      }
  return null
  }


  /***
    * return random edge count
    * @return random edge count
    */
  def getEdgeCount(): Int =
  {
    return getIndependentVariable(edgeDistro).toInt
  }

  /***
    *
    * @return random original byte count
    */
  def getOriginalByteCount(): Long = {

    return getIndependentVariable(origBytes)
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random original ip byte count
    */
  def getOriginalIPByteCount(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, origIPBytesCount, true).toLong
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random connection state
    */
  def getConnectState(byteCnt: Long): String =
  {

    return getDependentVariable(byteCnt.toLong, connectionState, false)
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random protocol
    */
  def getConnectType(byteCnt: Long): String =
  {

    return getDependentVariable(byteCnt.toLong, protocol, false)
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random duration
    */
  def getDuration(byteCnt: Long): Double =
  {

    return getDependentVariable(byteCnt.toLong, Duration, true).toDouble
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random original packet count
    */
  def getOriginalPackCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, origPacketCount, true).toLong
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random response byte count
    */
  def getRespByteCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, respByteCount, true).toLong
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random response ip byte count
    */
  def getRespIPByteCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, respIPByteCount, true).toLong
  }

  /***
    *
    * @param byteCnt the original byte count
    * @return random response packet count
    */
  def getRespPackCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, respPacketCount, true).toLong
  }

  /***
    *
    * @return randomly generated ip and port
    */
  def generateNodeData(): String = {
    val r = Random

    r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + ":" + r.nextInt(65536)
  }


  /***
    *
    * @return a nodeData with data randomly generated
    */
  private def generateNode(): nodeData = {
      nodeData(generateNodeData())
  }

  /***
    *
    * @param sc spark context
    * @param eRDD the edge RDD
    * @return the same edge RDD but the properties have been randomly generated
    */
  def generateEdgeProperties(sc: SparkContext, eRDD: RDD[Edge[edgeData]]): RDD[Edge[edgeData]] = {
    val gen_eRDD = eRDD.map(record => Edge(record.srcId, record.dstId, generateEdge()))

    gen_eRDD
  }

  /***
    *
    * @param sc spark context
    * @param vRDD the vertices RDD
    * @return the same vertices RDD but the randomly generated data
    */
  def generateNodeProperties(sc: SparkContext, vRDD: RDD[(VertexId, nodeData)]): RDD[(VertexId, nodeData)] = {

    val gen_vRDD = vRDD.map(record => (record._1, generateNode()))

    gen_vRDD
  }

  /***
    *
    * @return a randomly generated edgeData
    */
  private def generateEdge(): edgeData = {
    val ORIGBYTES = getOriginalByteCount()
    val ORIGIPBYTE = getOriginalIPByteCount(ORIGBYTES)
    val CONNECTSTATE = getConnectState(ORIGBYTES)
    val CONNECTTYPE = getConnectType(ORIGBYTES)
    val DURATION = getDuration(ORIGBYTES)
    val ORIGPACKCNT = getOriginalPackCnt(ORIGBYTES)
    val RESPBYTECNT = getRespByteCnt(ORIGBYTES)
    val RESPIPBYTECNT = getRespIPByteCnt(ORIGBYTES)
    val RESPPACKCNT = getRespPackCnt(ORIGBYTES)
    edgeData("", CONNECTTYPE, DURATION, ORIGBYTES, RESPBYTECNT, CONNECTSTATE, ORIGPACKCNT, ORIGIPBYTE, RESPPACKCNT, RESPBYTECNT, "")
  }

}

