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



  //constructor
  //THIS MUST BE CALLED FOR THE OBJECT TO FUNCTION PROPERLY
  def init(sparkSession: SparkSession)
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



  def getBucket(byteCount: Long): String =
  {
    val multiple = byteCount / bucketSize
    val beginBucket = (bucketSize * multiple).toString
    val endBucket = ((bucketSize * multiple) + bucketSize - 1).toString

    return beginBucket + "-" + endBucket
  }

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

  def randNumFromRange(str: String): Long =
  {
    val first = str.split("-")(0).toLong
    val last = str.split("-")(1).toLong
    val r = new Random()

    return r.nextInt(last.toInt - first.toInt) + first
  }


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



  def getEdgeCount(): Int =
  {
    return getIndependentVariable(edgeDistro).toInt
  }

  def getOriginalByteCount(): Long = {

    return getIndependentVariable(origBytes)
  }

  def getOriginalIPByteCount(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, origIPBytesCount, true).toLong
  }

  def getConnectState(byteCnt: Long): String =
  {

    return getDependentVariable(byteCnt.toLong, connectionState, false)
  }

  def getConnectType(byteCnt: Long): String =
  {

    return getDependentVariable(byteCnt.toLong, protocol, false)
  }

  def getDuration(byteCnt: Long): Double =
  {

    return getDependentVariable(byteCnt.toLong, Duration, true).toDouble
  }

  def getOriginalPackCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, origPacketCount, true).toLong
  }

  def getRespByteCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, respByteCount, true).toLong
  }

  def getRespIPByteCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, respIPByteCount, true).toLong
  }

  def getRespPackCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toLong, respPacketCount, true).toLong
  }

  def generateNodeData(): String = {
    val r = Random

    r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + ":" + r.nextInt(65536)
  }



  private def generateNode(): nodeData = {
      nodeData(generateNodeData())
  }

  def generateEdgeProperties(sc: SparkContext, eRDD: RDD[Edge[edgeData]]): RDD[Edge[edgeData]] = {
    val gen_eRDD = eRDD.map(record => Edge(record.srcId, record.dstId, generateEdge()))

    gen_eRDD
  }

  def generateNodeProperties(sc: SparkContext, vRDD: RDD[(VertexId, nodeData)]): RDD[(VertexId, nodeData)] = {

    val gen_vRDD = vRDD.map(record => (record._1, generateNode()))

    gen_vRDD
  }

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
    //val tempEdgeProp: edgeData = edgeData()
  }

}

