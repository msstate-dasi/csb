package edu.msstate.dasi.csb

import java.io._

import scala.util.Random

class DataDistributions(augLogPath: String) extends Serializable{
  private val bucketSize = 10

  private val fileDir = "."

//  private var inDegreeDistribution :Array[((String, Integer), Double)] = Array.empty
//  private var outDegreeDistribution :Array[((String, Integer), Double)] = Array.empty
//  private var degreeDistribution :Array[((String, Integer), Double)] = Array.empty

  private var inEdgesDistribution :Array[(Long, Double)] = Array.empty
  private var outEdgesDistribution :Array[(Long, Double)] = Array.empty
  private var origBytesDistribution :Array[(Long, Double)] = Array.empty

  private var origPktsDistributions: Map[Long, Array[(Long, Double)]] = Map.empty
  private var respBytesDistributions: Map[Long, Array[(Long, Double)]] = Map.empty
  private var durationDistributions: Map[Long, Array[(Double, Double)]] = Map.empty
  private var connStateDistributions: Map[Long, Array[(String, Double)]] = Map.empty
  private var protoDistributions: Map[Long, Array[(String, Double)]] = Map.empty
  private var origIPBytesDistributions: Map[Long, Array[(Long, Double)]] = Map.empty
  private var respIPBytesDistributions: Map[Long, Array[(Long, Double)]] = Map.empty
  private var respPktsDistributions: Map[Long, Array[(Long, Double)]] = Map.empty
  private var descriptionDistributions: Map[Long, Array[(String, Double)]] = Map.empty

  private val inEdgesDistributionFileName =     "inEdgesDistribution.ser"
  private val outEdgesDistributionFileName =     "outEdgesDistribution.ser"
  private val origBytesDistributionFileName =    "origBytesDistribution.ser"
  private val origPktsDistributionsFileName =    "origPktsDistributions.ser"
  private val respBytesDistributionsFileName =   "respBytesDistributions.ser"
  private val durationDistributionsFileName =    "durationDistributions.ser"
  private val connStateDistributionsFileName =   "connStateDistributions.ser"
  private val protoDistributionsFileName =       "protoDistributions.ser"
  private val origIPBytesDistributionsFileName = "origIPBytesDistributions.ser"
  private val respIPBytesDistributionsFileName = "respIPBytesDistributions.ser"
  private val respPktsDistributionsFileName =    "respPktsDistributions.ser"
  private val descriptionDistributionsFilename = "descDistributions.ser"


  if (new File(fileDir + "/" + inEdgesDistributionFileName).exists() &&
    new File(fileDir + "/" + outEdgesDistributionFileName).exists() &&
    new File(fileDir + "/" + origBytesDistributionFileName).exists() &&
    new File(fileDir + "/" + origPktsDistributionsFileName).exists() &&
    new File(fileDir + "/" + respBytesDistributionsFileName).exists() &&
    new File(fileDir + "/" + durationDistributionsFileName).exists() &&
    new File(fileDir + "/" + connStateDistributionsFileName).exists() &&
    new File(fileDir + "/" + protoDistributionsFileName).exists() &&
    new File(fileDir + "/" + origIPBytesDistributionsFileName).exists() &&
    new File(fileDir + "/" + respIPBytesDistributionsFileName).exists() &&
    new File(fileDir + "/" + respPktsDistributionsFileName).exists() &&
    new File(fileDir + "/" + descriptionDistributionsFilename).exists()
  // !gen_dist
  ) {
    readDistributionsFromDisk(fileDir)
  } else {
    if (inEdgesDistribution.isEmpty) {
      val augLogFile = sc.textFile(augLogPath)

      val augLogFiltered = augLogFile.mapPartitionsWithIndex { (idx, lines) => if (idx == 0) lines.drop(8) else lines }
        .filter(isInet4).filter(isAllowedProto)

      /* Cache augLog because it is the basis for any distribution computation */
      val augLog = augLogFiltered.map(line => parseAugLog(line)).persist()

//      // # of incoming edges per vertex
//      val inEdgesPerNode = augLog.map(entry => ( (entry.respIp, entry.respPort), 1L )).reduceByKey(_ + _).persist()
//      val inEdgesTotal = inEdgesPerNode.map(_._2).reduce(_ + _)
//      inDegreeDistribution = inEdgesPerNode.map(x => (x._1, x._2 / inEdgesTotal.toDouble)).sortBy(_._2, false).collect()
//
//      // # of outgoing edges per vertex
//      val outEdgesPerNode = augLog.map(entry => ( (entry.origIp, entry.origPort), 1L)).reduceByKey(_ + _).persist()
//      val outEdgesTotal = outEdgesPerNode.map(_._2).reduce(_ + _)
//      outDegreeDistribution = outEdgesPerNode.map(x => (x._1, x._2 / outEdgesTotal.toDouble)).sortBy(_._2, false).collect()
//
//      // # of incoming and outgoing edges per each vertex
//      val edgesPerNode = inEdgesPerNode.union(outEdgesPerNode).reduceByKey(_ + _)
//      val edgesTotal = inEdgesTotal + outEdgesTotal
//      degreeDistribution = edgesPerNode.map(x => (x._1, x._2 / edgesTotal.toDouble)).sortBy(_._2, false).collect()

//      outEdgesPerNode.unpersist()
//      inEdgesPerNode.unpersist()

      // # of edges per (respIp,respPort -> origIp,origPort)
      val inEdgesPerPair = augLog.map(entry => (( (entry.respIp, entry.respPort), (entry.origIp, entry.origPort) ), 1L)).reduceByKey(_ + _)
      val pairsPerInEdgeMultiplicity = inEdgesPerPair.map(x => (x._2, 1L)).reduceByKey(_ + _).sortBy(_._2, false).persist()
      val inPairsTotal = pairsPerInEdgeMultiplicity.map(_._2).reduce(_ + _)
      inEdgesDistribution = pairsPerInEdgeMultiplicity.map(x => (x._1, x._2 / inPairsTotal.toDouble)).sortBy(_._2, false).collect()
      pairsPerInEdgeMultiplicity.unpersist()

      // # of edges per (origIp,origPort -> respIp,respPort)
      val outEdgesPerPair = augLog.map(entry => (( (entry.origIp, entry.origPort), (entry.respIp, entry.respPort) ), 1L)).reduceByKey(_ + _)
      val pairsPerOutEdgeMultiplicity = outEdgesPerPair.map(x => (x._2, 1L)).reduceByKey(_ + _).sortBy(_._2, false).persist()
      val pairsTotal = pairsPerOutEdgeMultiplicity.map(_._2).reduce(_ + _)
      outEdgesDistribution = pairsPerOutEdgeMultiplicity.map(x => (x._1, x._2 / pairsTotal.toDouble)).sortBy(_._2, false).collect()
      pairsPerOutEdgeMultiplicity.unpersist()

      // Distribution of the number of origBytes
      val edgesPerOrigBytes = augLog.map(entry => (entry.origBytes, 1L)).reduceByKey(_ + _).persist()
      val outEdgesPerNode = augLog.map(entry => ( (entry.origIp, entry.origPort), 1L)).reduceByKey(_ + _)
      val outEdgesTotal = outEdgesPerNode.values.sum()
      val origBytesDistributionRDD = edgesPerOrigBytes.map(x => (x._1, x._2 / outEdgesTotal.toDouble)).sortBy(_._2, false)

      origBytesDistribution = origBytesDistributionRDD.collect()

      val origBytesList = origBytesDistributionRDD.map(_._1).collect()
      var totalsMap: Map[Long, Long] = Map()

      println("origPktsDistributions");
      {

        /* Conditional distribution of origPkts given the origBytes */
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.origPkts), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1 , entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          origPktsDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      println("respBytesDistributions");
      {
        /* Conditional distribution of respBytes given the origBytes */
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.respBytes), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          respBytesDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      /* Conditional distribution of connState given the origBytes */

      println("connStateDistributions");
      {
        /* Conditional distribution of respBytes given the origBytes */
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.connState), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          connStateDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      /* Conditional distribution of duration given the origBytes */ println("durationDistributions");
      {
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.duration), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          durationDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      /* Conditional distribution of proto given the origBytes */ println("protoDistributions");
      {
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.proto), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          protoDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      /* Conditional distribution of origIPBytes given the origBytes */ println("origIPBytesDistributions");
      {
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.origIpBytes), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          origIPBytesDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      /* Conditional distribution of respIPBytes given the origBytes */ println("respIPBytesDistributions");
      {
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.respIpBytes), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          respIPBytesDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      /* Conditional distribution of respPkts given the origBytes */ println("respPktsDistributions");
      {
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.respPkts), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1, entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          respPktsDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }

      println("descDistributions");
      {
        /* Conditional distribution of origPkts given the origBytes */
        //The following computes the sum of the occurrences of the packet counts given the byte counts
        val reducedByKey = augLog.map(entry => ((entry.origBytes, entry.desc), 1)).reduceByKey(_ + _)
        //The following sorts the count in descending order (it is now mapped as the second entry of the first element of the k,v tuple)
        val orderedData = reducedByKey.map(entry => ((entry._1._1 , entry._2), entry._1._2)).sortBy(_._1, false).persist()
        //The following computes the total of occurrences counted for each byte count
        val occTotalRdd = reducedByKey.map(x => (x._1._1, x._2)).reduceByKey(_ + _)
        val occTotalArray = occTotalRdd.collect()
        for (x <- occTotalArray) {
          totalsMap += (x._1 -> x._2)
        }
        for (origBytes <- origBytesList) {
          val occTotal = totalsMap(origBytes)
          val filteredOrderedData = orderedData.filter(entry => entry._1._1 == origBytes)
          descriptionDistributions += (origBytes -> filteredOrderedData.map(x => (x._2, x._1._2 / occTotal.toDouble)).collect())
        }
      }
    }
    writeDistributionsToDisk(fileDir)
  }

  private def isAllowedProto(line: String): Boolean = {
    val pieces = line.split('\t')
    pieces(6).contains("tcp") || pieces(6).contains("udp")
  }

  private def isInet4(line: String): Boolean = {
    val pieces = line.split('\t')
    ! ( pieces(2).contains(':') || pieces(4).contains(':') )
  }

  private case class AugLogLine(/* ts: java.util.Date, uid: String, */ origIp: String, origPort: Integer, respIp: String,
                        respPort: Integer, proto: String, /* service: String, */ duration: Double, origBytes: Long,
                        respBytes: Long, connState: String, /* localOrig: Boolean, localResp: Boolean, */
                        /* missedBytes: Long, history: String, */ origPkts: Long, origIpBytes: Long, respPkts: Long,
                        respIpBytes: Long, /* tunnelParents: String, */ desc: String)

  private def parseAugLog(line: String) = {
    val pieces = line.split('\t')
    // val tsPieces = pieces(0).split('.')
    // val ts = new java.util.Date(tsPieces(0).toLong * 1000)
    //val uid = pieces(1)
    val origIp = pieces(2)
    val origPort = pieces(3).toInt
    val respIp = pieces(4)
    val respPort = pieces(5).toInt
    val proto = pieces(6)
    //val service = pieces(7)
    val duration = pieces(8).toDouble
    val origBytes = pieces(9).toLong
    val respBytes = pieces(10).toLong
    val connState = pieces(11)
    //val localOrig = pieces(12).toBoolean
    //val localResp = pieces(13).toBoolean
    //val missedBytes = pieces(14).toLong
    //val history = pieces(15)
    val origPkts = pieces(16).toLong
    val origIpBytes = pieces(17).toLong
    val respPkts = pieces(18).toLong
    val respIpBytes = pieces(19).toLong
    //val tunnelParents = pieces(20)
    var desc     = ""
    if(pieces.length > 21)
      {
        desc = pieces(21)
      }
    AugLogLine(origIp, origPort, respIp, respPort, proto, duration, origBytes - origBytes % bucketSize,
      respBytes - respBytes % bucketSize, connState, origPkts - origPkts % bucketSize,
      origIpBytes - origIpBytes % bucketSize, respPkts - respPkts % bucketSize, respIpBytes - respIpBytes % bucketSize,
      desc)
  }

  def  writeDistributionsToDisk(fileDir: String) = {

    /* Not needed at this time
    var oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+inDegreeDistributionFileName))
    oos.writeObject(inDegreeDistribution)
    oos.close

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+outDegreeDistributionFileName))
    oos.writeObject(outDegreeDistribution)
    oos.close

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+degreeDistributionFileName))
    oos.writeObject(degreeDistribution)
    oos.close

    */

    var oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+inEdgesDistributionFileName))
    oos.writeObject(inEdgesDistribution)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+outEdgesDistributionFileName))
    oos.writeObject(outEdgesDistribution)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+origBytesDistributionFileName))
    oos.writeObject(origBytesDistribution)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+origPktsDistributionsFileName))
    oos.writeObject(origPktsDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+respBytesDistributionsFileName))
    oos.writeObject(respBytesDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+durationDistributionsFileName))
    oos.writeObject(durationDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+connStateDistributionsFileName))
    oos.writeObject(connStateDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+protoDistributionsFileName))
    oos.writeObject(protoDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+origIPBytesDistributionsFileName))
    oos.writeObject(origIPBytesDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+respIPBytesDistributionsFileName))
    oos.writeObject(respIPBytesDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+respPktsDistributionsFileName))
    oos.writeObject(respPktsDistributions)
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream(fileDir+"/"+descriptionDistributionsFilename))
    oos.writeObject(descriptionDistributions)
    oos.close()
  }

  def readDistributionsFromDisk(fileDir: String) = {

//    var ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+inDegreeDistributionFileName))
//    inDegreeDistribution = ois.readObject().asInstanceOf[Array[(String, Double)]]
//    ois.close()
//
//    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+outDegreeDistributionFileName))
//    outDegreeDistribution = ois.readObject().asInstanceOf[Array[(String, Double)]]
//    ois.close()
//
//    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+degreeDistributionFileName))
//    degreeDistribution = ois.readObject().asInstanceOf[Array[(String, Double)]]
//    ois.close()

    var ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+inEdgesDistributionFileName))
    inEdgesDistribution = ois.readObject().asInstanceOf[Array[(Long, Double)]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+outEdgesDistributionFileName))
    outEdgesDistribution = ois.readObject().asInstanceOf[Array[(Long, Double)]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+origBytesDistributionFileName))
    origBytesDistribution = ois.readObject().asInstanceOf[Array[(Long, Double)]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+origPktsDistributionsFileName))
    origPktsDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(Long, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+respBytesDistributionsFileName))
    respBytesDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(Long, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+durationDistributionsFileName))
    durationDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(Double, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+connStateDistributionsFileName))
    connStateDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(String, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+protoDistributionsFileName))
    protoDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(String, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+origIPBytesDistributionsFileName))
    origIPBytesDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(Long, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+respIPBytesDistributionsFileName))
    respIPBytesDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(Long, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+respPktsDistributionsFileName))
    respPktsDistributions = ois.readObject().asInstanceOf[Map[Long, Array[(Long, Double)]]]
    ois.close()

    ois = new ObjectInputStream(new FileInputStream(fileDir+"/"+descriptionDistributionsFilename))
      descriptionDistributions= ois.readObject().asInstanceOf[Map[Long, Array[(String, Double)]]]
    ois.close()
  }

  def getInEdgeSample: Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0

    val iterator = inEdgesDistribution.iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getOutEdgeSample: Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0

    val iterator = outEdgesDistribution.iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getOrigBytesSample: Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = origBytesDistribution.iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getOrigPktsSample(origBytes: Long) : Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = origPktsDistributions(origBytes).iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getRespBytesSample(origBytes: Long) : Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = respBytesDistributions(origBytes).iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getDurationSample(origBytes: Long) : Double = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = durationDistributions(origBytes).iterator
    var outElem : (Double, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getConnectionStateSample(origBytes: Long) : String = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = connStateDistributions(origBytes).iterator
    var outElem : (String, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getProtoSample(origBytes: Long) : String = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = protoDistributions(origBytes).iterator
    var outElem : (String, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getOrigIPBytesSample(origBytes: Long) : Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = origIPBytesDistributions(origBytes).iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getRespIPBytesSample(origBytes: Long) : Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = respIPBytesDistributions(origBytes).iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getRespPktsSample(origBytes: Long) : Long = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = respPktsDistributions(origBytes).iterator
    var outElem : (Long, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }

  def getDescSample(origBytes: Long): String = {
    val r = Random.nextDouble()
    var accumulator :Double= 0
    val iterator = descriptionDistributions(origBytes).iterator
    var outElem : (String, Double) = null
    while (accumulator < r && iterator.hasNext) {
      outElem = iterator.next()
      accumulator = accumulator + outElem._2
    }
    outElem._1
  }
}
