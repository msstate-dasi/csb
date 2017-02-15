package edu.msstate.dasi.csb

/**
 *
 *
 * @param proto Level 3 Protocol
 * @param duration
 * @param origBytes Original Bytes sent from source to destination
 * @param respBytes Response Bytes sent from destination to source
 * @param connState Connection State at termination of the connection
 * @param origPkts Original Packets sent from source to destination
 * @param origIpBytes Original IP Packet Bytes sent from source to destination
 * @param respPkts  Response Packets sent from destination to source
 * @param respIpBytes  Response IP Bytes sent from destination to source
 * @param desc Connection description
 */
case class EdgeData(/* ts: Date, */
                    /* uid: String */
                    proto: String = "",
                    /* service: String, */
                    duration: Double = Double.MinValue,
                    origBytes: Long = Long.MinValue,
                    respBytes: Long = Long.MinValue,
                    connState: String = "",
                    /* localOrig: Boolean, */
                    /* localResp: Boolean, */
                    /* missedBytes: Long, */
                    /* history: String, */
                    origPkts: Long = Long.MinValue,
                    origIpBytes: Long = Long.MinValue,
                    respPkts: Long = Long.MinValue,
                    respIpBytes: Long = Long.MinValue,
                    /* tunnelParents: String, */
                    desc: String = "") {
  /**
    * NOTE: FOR THIS FORMULA TO WORK THE THIS VARIABLE MUST BE THE FILTER AND THE THAT MUST BE THE EDGE THAT WE ARE TESTING TO SEE IF IT FITS THE FILTER
    * @param that the edge that is tested to see if it fits the filter
    * @return if the edge fits the filter
    */
  def <(that: EdgeData): Boolean =

  (this.proto == "")  && //cant really have a range on protocol
    ((this.duration == Double.MinValue ) || (this.duration < that.duration)) &&
    ((this.origBytes == Long.MinValue ) ||   (this.origBytes < that.origBytes)) &&
    ((this.respBytes == Long.MinValue ) ||    (this.respBytes < that.respBytes)) &&
    (this.connState == "" ) && //cant really have a string range
    ((this.origPkts == Long.MinValue ) ||    (this.origPkts < that.origPkts)) &&
    ((this.origIpBytes == Long.MinValue ) ||    (this.origIpBytes < that.origIpBytes)) &&
    ((this.respPkts == Long.MinValue ) ||    (this.respPkts < that.respPkts)) &&
    ((this.respIpBytes == Long.MinValue ) ||    (this.respIpBytes < that.respIpBytes)) &&
    (this.desc == "" )

  /**
    * NOTE: FOR THIS FORMULA TO WORK THE THIS VARIABLE MUST BE THE FILTER AND THE THAT MUST BE THE EDGE THAT WE ARE TESTING TO SEE IF IT FITS THE FILTER
    * @param that the edge that is tested to see if it fits the filter
    * @return if the edge fits the filter
    */
  def >(that:EdgeData): Boolean = !(this < that) && !(this == that)

  /**
    * NOTE: FOR THIS FORMULA TO WORK THE THIS VARIABLE MUST BE THE FILTER AND THE THAT MUST BE THE EDGE THAT WE ARE TESTING TO SEE IF IT FITS THE FILTER
    * @param that the edge that is tested to see if it fits the filter
    * @return if the edge fits the filter
    */
  def ~=(that: EdgeData): Boolean =
  {
    ((this.proto != "" &&  this.proto.equals(that.proto)) || this.proto == "") &&
      ((this.connState != "" && this.connState.equals(that.connState)) || this.connState == "") &&
      ((this.desc != "" && this.desc.equals(that.desc)) || this.desc == "") &&
      ((this.duration != Double.MinValue && this.duration.equals(that.duration)) || this.duration == Double.MinValue) &&
      ((this.origBytes != Long.MinValue && this.origBytes.equals(that.origBytes)) || this.origBytes == Long.MinValue) &&
      ((this.origIpBytes != Long.MinValue && this.origIpBytes.equals(that.origIpBytes)) || this.origIpBytes == Long.MinValue) &&
      ((this.origPkts != Long.MinValue && this.origPkts.equals(that.origPkts)) || this.origPkts == Long.MinValue) &&
      ((this.respBytes != Long.MinValue && this.respBytes.equals(that.respBytes)) || this.respBytes == Long.MinValue) &&
      ((this.respIpBytes != Long.MinValue && this.respIpBytes.equals(that.respIpBytes)) || this.respIpBytes == Long.MinValue) &&
      ((this.respPkts != Long.MinValue && this.respPkts.equals(that.respPkts)) || this.respPkts == Long.MinValue)
  }

  def toNeo4jString: String = {
    s"proto:'$proto', duration:$duration, origBytes:$origBytes, respBytes:$respBytes, connState:'$connState', " +
      s"origPkts:$origPkts, origIpBytes:$origIpBytes, respPkts:$respPkts, respIpBytes:$respIpBytes, desc:'$desc'"
  }
}

object EdgeData {
  /**
   *
   */
  def apply(text: String): EdgeData = {
    if (text == "null") {
      null.asInstanceOf[EdgeData]
    } else {
      // EdgeData example: EdgeData(udp,0.003044,116,230,SF,2,172,2,286,)
      val dataRegex = "\\w+\\(|[,)]"

      text.replaceFirst("^" + dataRegex, "").split(dataRegex) match {
        case Array(proto, duration, origBytes, respBytes, connState, origPkts, origIpBytes, respPkts, respIpBytes, desc) =>
          new EdgeData(
            proto,
            duration.toDouble,
            origBytes.toLong,
            respBytes.toLong,
            connState,
            origBytes.toLong,
            origIpBytes.toLong,
            respPkts.toLong,
            respIpBytes.toLong,
            desc
          )
        // TODO: check why we need the following, i.e. why might "desc" be empty?
        case Array(proto, duration, origBytes, respBytes, connState, origPkts, origIpBytes, respPkts, respIpBytes) =>
          new EdgeData(
            proto,
            duration.toDouble,
            origBytes.toLong,
            respBytes.toLong,
            connState,
            origBytes.toLong,
            origIpBytes.toLong,
            respPkts.toLong,
            respIpBytes.toLong
          )
      }
    }
  }
}