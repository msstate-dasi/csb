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

object data_Generator extends Serializable {

//I keep the spark variable here in case we want to change how we select from the database
  //ie. I use df like so df.select("blablabla")
  //whereas you could do this:
  // spark.read.json("file.json").createOrReplaceTempView("df")
  //spark.sql("ENTER SQL COMMAND HERE")
var spark: SparkSession = null
var df: sql.DataFrame = null

  //constructor
  //THIS MUST BE CALLED FOR THE OBJECT TO FUNCTION PROPERLY
  def init(sparkSession: SparkSession)
  {
    spark = sparkSession
    df = this.spark.read.json("seed_distributions.json")
    df.persist()
  }

  /***
    * This function returns the bucket that a particular number is in.
    * Example: If we pass in 15 and we have buckets of length 10(0-9, 10-19, ...)
    * then this function will return the string "10-19" and we use that string in
    * looking up things in the dataframe.
    * NOTE: this function grabs the buckets from the database so no need to specify
    * @param str The root directory of the dataframe Ex. : ORIG_BYTES
    * @param OriginalByteCnt The original byte count whose bucket you wanna know it fits in.
    * @return
    */
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
    * in the json file.  It is independent because it does not matter what
    * Original_Bytes is.
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
      var selectStr: String = ""


      //the following if/else statement if for sql syntax
      //if subStr is "" we dont want a trailing period
      if(subStr.length > 0)
      {
        selectStr = root + "." + strings(i) + "." + subStr
      }
      else
      {
          selectStr = root + "." + strings(i)
      }


      //The try catch lines has to do with a weird java runtime exception
      //on the off chance the percentage for a particular thing is 1
      //meaning that it is the only option, java won't convert that to a double
      //enter the catch...it just sets sum to 1 which is expected behavior
      try
      {
        sum = sum + df.select(selectStr).head().getDouble(0)
      }
      catch
        {
          case cce: ClassCastException => sum = 1
        }

      //This if is when we have found the value we are going with "randomly generated" value
      if(sum >= randNum)
      {

        //This function is called for Original_bytes and Edge_DIST
        //which has different outputs
        //EX: EDGE_DIST => 2
        //EX: ORIG_BYTES => 10-19
        //These try catch statements take care of that
        //try gets edge_dist
        //catch gets ORIG_BYTES
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


  /***
    * This function returns a random number given a range
    * @param str string in the format "a-b" where a and b are integers
    * @return
    */
  def randNumFromRange(str: String): Int =
  {
    val first = str.split("-")(0).toInt
    val last = str.split("0-")(1).toInt
    val r = Random

    return r.nextInt(last - first) + first
  }


  /***
    * This function is responsible for searching the database and picking
    * a random value for all the properties that are dependent on original bytes
    * @param byteNum the original byte count that acts as the variable this function is dependent on
    * @param root the root of the database
    * @param subStr
    * @return
    */
  def getDependentVariable(byteNum: Int, root: String, subStr: String): String =
  {
    val r : Random = Random
    val randNum = r.nextDouble()
    val bucket: String = getBucket("ORIG_BYTES", byteNum)

    val columns = df.select(root + "." + bucket + "." + subStr + ".*").columns

    var sum = 0.0
    for(i <- 0 to columns.length)
      {

        //this try and catch is because if we can't convert the value that
        //is found in the dataframe to a double IF it is a 1.
        //In other words the chance for that particular value was 100%
        //but java can't convert a Long 1 (Thats what java thinks it is)
        //to a double 1.0
        try {
          sum = sum + df.select(root + "." + bucket + "." + subStr + "." + columns(i)).head().getDouble(0)
        }
        catch
          {
            case cce: ClassCastException =>
            {
              sum = 1
            }
          }

        //when this is true we have picked out our random value
        if(sum >= randNum)
          {
            //given the way our data is stored for our dependent values
            //we will either get something in the format
            // "a-b" where a and b are integers
            //or just a string like "tcp"
            //this try and catch handles both cases.
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


  /***
    * get random edge count
    * @return random edge count
    */
  def getEdgeCount(): Int =
  {
    return getIndependentVariable("EDGE_DIST", "")
  }

  /***
    * get random original byte count
    * @return random original byte count
    */
  def getOriginalByteCount(): Long = {

    return getIndependentVariable("ORIG_BYTES","DIST")
  }

  /***
    * return randome originalIPByteCount
    * @param byteCnt original byte count
    * @return random original ip byte count
    */
  def getOriginalIPByteCount(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "ORIG_IP_BYTES").toLong
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random connection state
    */
  def getConnectState(byteCnt: Long): String =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "CONN_STATE")
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random protocol
    */
  def getConnectType(byteCnt: Long): String =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "PROTOCOL")
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random duration
    */
  def getDuration(byteCnt: Long): Double =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "DURATION").toDouble
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random original packet count
    */
  def getOriginalPackCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "ORIG_PKTS").toLong
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random response byte count
    */
  def getRespByteCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "RESP_BYTES").toLong
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random response IP byte count
    */
  def getRespIPByteCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "RESP_IP_BYTES").toLong
  }

  /***
    *
    * @param byteCnt original byte count
    * @return random response packet count
    */
  def getRespPackCnt(byteCnt: Long): Long =
  {

    return getDependentVariable(byteCnt.toInt, "ORIG_BYTES", "RESP_PKTS").toLong
  }

  /***
    * generates random node data (basically ip and port)
    * @return String in the format x.x.x.x:port
    */
  def generateNodeData(): String = {
    val r = Random

    r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + "." + r.nextInt(255) + ":" + r.nextInt(65536)
  }


  /***
    * wrapper function to  generate node data
    * @return a random nodeData object
    */
  private def generateNode(): nodeData = {
      nodeData(generateNodeData())
  }

  /***
    * This function generates random data given an edgelist.
    * It is completely parrallel so it should be fast.
    * @param sc the current spark context
    * @param eRDD the edge RDD that you want to generate random data for
    * @return the edge RDD that now has random data
    */
  def generateEdgeProperties(sc: SparkContext, eRDD: RDD[Edge[edgeData]]): RDD[Edge[edgeData]] = {
    val gen_eRDD = eRDD.map(record => Edge(record.srcId, record.dstId, generateEdge()))

    gen_eRDD
  }

  /***
    * This function generates random data given a vertices list.
    * It is completely parralell so it should be fast.
    * @param sc the current spark context
    * @param vRDD the vert RDD that you want to generate random data for
    * @return the vert RDD that now has random data
    */
  def generateNodeProperties(sc: SparkContext, vRDD: RDD[(VertexId, nodeData)]): RDD[(VertexId, nodeData)] = {

    val gen_vRDD = vRDD.map(record => (record._1, generateNode()))

    gen_vRDD
  }

  /***
    * This acts as a wrapper function to call every function that gerates random data
    * @return a random edgeData object that you can attach to an edge
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

