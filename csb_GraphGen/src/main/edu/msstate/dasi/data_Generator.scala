package edu.msstate.dasi

import org.apache.spark.{SparkContext, sql}
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.util.Random

/**
  * Created by justin on 12/5/2016.
  */

import scala.io.Source

class data_Generator(val spark: SparkSession) extends Serializable {
  var edgeCnt: String = ""
  var originalBytesStr: String = ""
  var originalIPByteCntStr: String = ""
  var connectionStateStr: String = ""
  var connectionTypeStr: String = ""
  var durationStr: String = ""
  var originalPackCntStr: String = ""
  var respByteCntStr: String = ""
  var respIPByteCntStr: String = ""
  var respPackCntStr: String = ""
  var this.spark = spark
  val df : sql.DataFrame = this.spark.read.json("seed_distributions.json")


  //constructor
  {
    df.persist()
    val originalBytes = df.select("ORIG_BYTES").createOrReplaceTempView("originalBytes")
    val edgeDist = df.select("EDGE_DIST").createOrReplaceTempView("EDGE_DIST")

//    println(df.select("ORIG_BYTES.*").columns(1))












//    this.edgeCnt =              readFile("Edge_distributions")
//    this.originalBytesStr =     readFile("Original_byte_count")
//    this.originalIPByteCntStr = readFile("Original_IP_byte_count")
//    this.connectionStateStr =   readFile("Connection_state")
//    this.connectionTypeStr =    readFile("Connection_type")
//    this.durationStr =          readFile("Duration_of_connection")
//    this.originalPackCntStr =   readFile("Original_packet_count")
//    this.respByteCntStr =       readFile("Resp_byte_count")
//    this.respIPByteCntStr =     readFile("Resp_IP_byte_count")
//    this.respPackCntStr =       readFile("Resp_packet_count")
  }


  def readFile(fileName: String): String = {
    return Source.fromFile(fileName).mkString
  }

  def getBucket(str : String, OriginalByteCnt: Long): String =
  {
    val headers = df.select(str + ".*").columns
    for( i <- 0 to headers.length - 1)
      {
        if(OriginalByteCnt >= headers(i).split("-")(0).toLong && OriginalByteCnt <= headers(i).split("-")(1).toLong)
          {
            return headers(i)
          }
      }
    return null
  }

  /***
    * This function returns a randomized int based on the probabilities given
    * in the json file.
    * @param root This is the root directory of the JSON Example EDGE_DIST
    * @param subStr This is to take care in case the directory you go to has another directory inside it for the actual distribution
    *               Example ORIG_BYTES.0-9.DIST
    *               the 0-9 is a seperate original byte count range but to access the DIST you need the subStr to specify it.
    * @return
    */
  def getIndependentVariable(root: String, subStr: String): Int =
  {
    val r : Random = Random
    val randNum = r.nextDouble()

    val strings = df.select(root + ".*").columns
    var sum: Double = 0
    for( i <- 0 to strings.length - 1)
    {
      if(subStr.length > 0) sum = sum + df.select(root + "." + strings(i) + "." + subStr).head().getDouble(0)
      else sum = sum + df.select(root + "." + strings(i)).head().getDouble(0)
      if(sum >= randNum)
      {
        //EDGE DISTRIBUTION LOOKUP
        try
        {
          return strings(i).toInt
        }
        catch
          {
            case nfe: NumberFormatException => return randNumFromRange(strings(i))
          }

      }
    }


    return 1
  }

  def randNumFromRange(str: String): Int =
  {
    val first = str.split("-")(0).toInt
    val last = str.split("0-")(1).toInt
    val r = Random

    return r.nextInt(last - first) + first
  }


  def getDependentVariable(byteNum: Int, root: String, subStr: String): String =
  {
    val r : Random = Random
    val randNum = r.nextDouble()
    val bucket: String = getBucket("ORIG_BYTES", byteNum)

    val columns = df.select(root + "." + bucket + "." + subStr + ".*").columns

    var sum = 0.0
    for(i <- 0 to columns.length)
      {
        println(root + "." + bucket + "." + subStr + "." + columns(i))
        try {
          sum = sum + df.select(root + "." + bucket + "." + subStr + "." + columns(i)).head().getDouble(0)
        }
        catch
          {
            case cce: ClassCastException =>
            {
//              println("THE NEXT THING BETTER BE A 1 " + df.select(root + "." + bucket + "." + subStr + "." + columns(i)).head())
              sum = 1
            }
          }

        if(sum >= randNum)
          {
            try
            {
              return randNumFromRange(columns(i)).toString
            }
            catch
              {
                case nfe: NumberFormatException => return columns(i)
              }

          }
      }
    return null
  }



  def getEdgeCount(): Int =
  {
    return getIndependentVariable("EDGE_DIST", "")
  }

  def getOriginalByteCount(): Long = {
    return getIndependentVariable("ORIG_BYTES","DIST")
  }

  def getOriginalIPByteCount(byteCnt: Long): Long =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "ORIG_IP_BYTES").toLong
  }

  def getConnectState(byteCnt: Long): String =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "CONN_STATE")
  }

  def getConnectType(byteCnt: Long): String =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "PROTOCOL")
  }

  def getDuration(byteCnt: Long): Double =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "DURATION").toDouble
  }

  def getOriginalPackCnt(byteCnt: Long): Long =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "ORIG_PKTS").toLong
  }

  def getRespByteCnt(byteCnt: Long): Long =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "RESP_BYTES").toLong
  }

  def getRespIPByteCnt(byteCnt: Long): Long =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "RESP_IP_BYTES").toLong
  }

  def getRespPackCnt(byteCnt: Long): Long =
  {
    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "RESP_PKTS").toLong
  }

  def generateNodeData(): String = {
    val r = Random

    r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + ":" + r.nextInt(65536)
  }

  /** *
    * This function reads a distrobution that is not based on Original Bytes (independent of anything)
    * and picks a number based on the distribution.
    *
    * @param fileContents the contents of one of the files is generated by examining a conn.log file.
    * @return a random number based on a distrobution
    */
  def generateRandNumFromFileDist(fileContents: String): Int = {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0
    var strIter = fileContents.split("\n").toIterator
    while (strIter.hasNext && num == 0) {
      val line = strIter.next()
      val percentage = line.split("\\*")(1).split("\t")(1).toFloat
      chance = chance + percentage
      if (chance > numEdgesProb) {
        if (!line.split("\t")(0).contains("-")) {
          num = line.split("\t")(0).split("\\*")(1).toInt //split is a reg expression function so i escape the *
        }
        else {
          val firstTabLine = line.split("\t")(0)
          val begin: Int = firstTabLine.split("\\*")(1).split("-")(0).toInt
          val end: Int = firstTabLine.split("\\*")(1).split("-")(1).toInt
          num = r.nextInt(end - begin) + begin
        }
      }
    }
    num
  }

  /** *
    * This function returns a random number but with respect to the number of Original Bytes the connection had.
    * This is to prevent randomly generating bad data such as:
    * Original Bytes: 2GB
    * NumPackets: 2
    *
    * @param fileContents the contents of one of the files is generated by examining a conn.log file.
    * @param byteNum      The number of bytes this connection had.
    * @param sc           A spark context
    * @return
    */
  def generateRandNumBasedBytes(fileContents: String, byteNum: Long, sc: SparkContext): Long = {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0

    val splitFileContents = sc.parallelize(fileContents.split("\n"))
    var text = splitFileContents.filter(record => record.split("\\*")(0).split("-")(0).toLong <= byteNum && record.split("\\*")(0).split("-")(1).toLong >= byteNum)

    //    var allStr = ""
    //
    //    for (aStr <- text.collect())
    //    {
    //      allStr = allStr + aStr + "\n"
    //    }

    //    var temp = new FileWriter("temp")
    //
    //    temp.write(allStr)
    //    temp.close()

    var line: String = generateRandStrFromFileDist(text.toLocalIterator)

    val range: String = line.split("\t").head.split("\\*")(1)

    val begin: Int = range.split("-").head.toInt
    val end: Int = range.split("-")(1).toInt

    return r.nextInt(end - begin) + begin
  }

  def generateRandDoubleWithinRange(range: String): Double = {
    val begin = range.split("-")(0).toInt
    val end = range.split("-")(1).toInt

    val r = Random
    val decimal = r.nextDouble()
    val digit = r.nextInt(end - begin) + begin
    return digit + decimal
  }

  def generateRandStrBasedBytes(fileContents: String, byteNum: Long, sc: SparkContext): String = {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0
    var rdd = sc.parallelize(fileContents.split("\n"))
    //    fileIter.next() //we do the next here since the heading is always just text describeing the file


    //    var file = sc.textFile(filename)
    var text = rdd.filter(record => record.split("\\*")(0).split("-")(0).toLong <= byteNum && record.split("\\*")(0).split("-")(1).toLong >= byteNum)

    //
    //    var allStr = ""
    //
    //    for (aStr <- text.collect())
    //    {
    //      allStr = allStr + aStr + "\n"
    //    }
    //
    //
    //    var temp = new FileWriter("temp")
    //
    //    temp.write(allStr)
    //    temp.close()

    val line = generateRandStrFromFileDist(text.toLocalIterator)

    return line.split(" ").head.split("\\*")(1)
  }

  /** *
    * Given a set of of lines pick one at random
    * This function is very similar to generateRandNumFromFileDist
    * but instead of getting a number this function returns the line that it picks
    *
    * @param fileIter : The contents of the file in a string iter
    * @return a weighted random line
    */
  def generateRandStrFromFileDist(fileIter: Iterator[String]): String = {
    val r = new Random
    val numEdgesProb = r.nextFloat()
    var chance = 0.0
    var num = 0


    while (fileIter.hasNext && num == 0) {
      val line = fileIter.next()
      val percentage = line.split("\t")(1).toFloat
      chance = chance + percentage
      if (chance > numEdgesProb) {
        return line
      }
    }
    return null
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
    val sc: SparkContext = new SparkContext()
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

