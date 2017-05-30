package edu.msstate.dasi.csb

/**
 * Defines the edge data which represents the connection properties.
 *
 * @param ts          The timestamp of the first packet.
 * @param origPort    The originator’s port number.
 * @param respPort    The responder’s port number.
 * @param proto       The transport layer protocol of the connection.
 * @param duration    How long the connection lasted. For 3-way or 4-way connection tear-downs, this will not include
 *                    the final ACK.
 * @param origBytes   The number of payload bytes the originator sent. For TCP this is taken from sequence numbers and
 *                    might be inaccurate (e.g., due to large connections).
 * @param respBytes   The number of payload bytes the responder sent. See [[origBytes]].
 * @param connState
 *                    S0      Connection attempt seen, no reply.
 *                    S1      Connection established, not terminated.
 *                    SF      Normal establishment and termination. Note that this is the same symbol as for state S1.
 *                            You can tell the two apart because for S1 there will not be any byte counts in the
 *                            summary, while for SF there will be.
 *                    REJ     Connection attempt rejected.
 *                    S2      Connection established and close attempt by originator seen (but no reply from responder).
 *                    S3      Connection established and close attempt by responder seen (but no reply from originator).
 *                    RSTO    Connection established, originator aborted (sent a RST).
 *                    RSTR    Responder sent a RST.
 *                    RSTOS0  Originator sent a SYN followed by a RST, we never saw a SYN-ACK from the responder.
 *                    RSTRH   Responder sent a SYN ACK followed by a RST, we never saw a SYN from the (purported)
 *                            originator.
 *                    SH      Originator sent a SYN followed by a FIN, we never saw a SYN ACK from the responder
 *                            (hence the connection was “half” open).
 *                    SHR     Responder sent a SYN ACK followed by a FIN, we never saw a SYN from the originator.
 *                    OTH     No SYN seen, just midstream traffic (a “partial connection” that was not later closed).
 * @param origPkts    Number of packets that the originator sent.
 * @param origIpBytes Number of IP level bytes that the originator sent (as seen on the wire, taken from the IP
 *                    total_length header field).
 * @param respPkts    Number of packets that the responder sent.
 * @param respIpBytes Number of IP level bytes that the responder sent (as seen on the wire, taken from the IP
 *                    total_length header field).
 * @param desc        Connection description
 */
case class EdgeData(ts: Long,
                    /* uid: String */
                    origPort: Int,
                    respPort: Int,
                    proto: String,
                    /* service: String, */
                    duration: Double,
                    origBytes: Long,
                    respBytes: Long,
                    connState: String,
                    /* localOrig: Boolean, */
                    /* localResp: Boolean, */
                    /* missedBytes: Long, */
                    /* history: String, */
                    origPkts: Long,
                    origIpBytes: Long,
                    respPkts: Long,
                    respIpBytes: Long,
                    /* tunnelParents: String, */
                    desc: String) {
  def toCsv: String = s"$ts,$origPort,$respPort,$proto,$duration,$origBytes,$respBytes,$connState,$origPkts," +
    s"$origIpBytes,$respPkts,$respIpBytes,$desc"
}

object EdgeData {
  /**
   *
   */
  def apply(text: String): EdgeData = {
    if (text == "null") {
      null.asInstanceOf[EdgeData]
    } else {
      // EdgeData example: EdgeData(1318226897,68,67,udp,0.003044,116,230,SF,2,172,2,286,)
      val dataRegex = "\\w+\\(|[,)]"

      text.replaceFirst("^" + dataRegex, "").split(dataRegex) match {
        case Array(ts, origPort, respPort, proto, duration, origBytes, respBytes, connState, origPkts, origIpBytes,
        respPkts, respIpBytes, desc) =>
          new EdgeData(
            ts.toLong,
            origPort.toInt,
            respPort.toInt,
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
        case Array(ts, origPort, respPort, proto, duration, origBytes, respBytes, connState, origPkts, origIpBytes,
        respPkts, respIpBytes) =>
          new EdgeData(
            ts.toLong,
            origPort.toInt,
            respPort.toInt,
            proto,
            duration.toDouble,
            origBytes.toLong,
            respBytes.toLong,
            connState,
            origPkts.toLong,
            origIpBytes.toLong,
            respPkts.toLong,
            respIpBytes.toLong,
            ""
          )
      }
    }
  }

  def neo4jCsvHeader: String = "ts:long,origPort:int,respPort:int,proto,duration:double,origBytes:long," +
    "respBytes:long,connState,origPkts:long,origIpBytes:long,respPkts:long,respIpBytes:long,desc"
}