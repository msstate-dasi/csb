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


  def toNeo4jString: String = s"proto:'$proto', duration:$duration, origBytes:$origBytes, respBytes:$respBytes, " +
    s"connState:'$connState', origPkts:$origPkts, origIpBytes:$origIpBytes, respPkts:$respPkts, " +
    s"respIpBytes:$respIpBytes, desc:'$desc'"

  def toCsv: String = s"$proto,$duration,$origBytes,$respBytes,$connState,$origPkts,$origIpBytes,$respPkts," +
    s"$respIpBytes,$desc"

  def toMap: Map[String, Any] = Map("proto" -> proto, "duration" -> duration, "origBytes" -> origBytes,
    "respBytes" -> respBytes, "connState" -> connState, "origPkts" -> origPkts, "origIpBytes" -> origIpBytes,
    "respPkts" -> respPkts, "respIpBytes" -> respIpBytes, "desc" -> desc)
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
            origPkts.toLong,
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
            origPkts.toLong,
            origIpBytes.toLong,
            respPkts.toLong,
            respIpBytes.toLong
          )
      }
    }
  }

  def toNullMap: Map[String, Any] = Map("proto" -> null, "duration" -> null, "origBytes" -> null, "respBytes" -> null,
    "connState" -> null, "origPkts" -> null, "origIpBytes" -> null, "respPkts" -> null, "respIpBytes" -> null,
    "desc" -> null)

  def neo4jQueryTemplate(prefix: String): String = s"proto: $prefix.proto, duration: $prefix.duration, " +
    s"origBytes: $prefix.origBytes, respBytes: $prefix.respBytes, connState: $prefix.connState, " +
    s"origPkts: $prefix.origPkts, origIpBytes: $prefix.origIpBytes, respPkts: $prefix.respPkts, " +
    s"respIpBytes: $prefix.respIpBytes, desc: $prefix.desc"

  def neo4jCsvHeader: String = "proto,duration:double,origBytes:long,respBytes:long,connState,origPkts:long," +
    "origIpBytes:long,respPkts:long,respIpBytes:long,desc"
}